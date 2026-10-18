import { Injectable, Logger, Optional } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import {
  BlockRecord,
  PendingEvent,
  ReorgEvent,
  ChainState,
  BlockInfo,
  StateMemoryStats,
  IndexerHealthSnapshot,
  IndexerHealthStatus,
  ReadinessSnapshot,
  ReadinessStatus,
  DependencyProvenance,
} from './types';

/**
 * Maximum age of a successful observation before readiness is withdrawn.
 * A stale last-success cannot report healthy.
 */
const DEFAULT_STANE_THRESHOLD_MS = 60_000;

export const READINESS_STATUS_PREFIX = 'readiness';

@Injectable()
export class BlockchainStateService {
  private readonly logger = new Logger(BlockchainStateService.name);

  private blocks: Map<string, BlockRecord> = new Map();
  private events: Map<string, PendingEvent> = new Map();
  private reorgHistory: ReorgEvent[] = [];
  private chainState: ChainState = {
    lastProcessedBlock: 0,
    lastCanonicalHash: '',
    confirmedDepth: 0,
    pendingEventCount: 0,
    orphanedEventCount: 0,
    rpcFailureCount: 0,
    replayCount: 0,
    deadLetterCount: 0,
    lastRpcErrorAt: null,
    projectionLag: 0,
  };

  private readonly maxBlocksInMemory: number;
  private readonly maxEventsInMemory: number;
  private readonly maxReorgHistoryEntries: number;

  // Alert thresholds (configurable, with safe defaults).
  private readonly indexerLagThresholdBlocks: number;
  private readonly finalityLagThresholdBlocks: number;
  private readonly rpcFailureWindow: number;
  private readonly rpcFailureLimit: number;
  private readonly maxDeadLetters: number;
  private readonly rpcFailureTimestamps: number[] = [];
  private readonly runbookUrl: string;

  // Readiness / provenance tracking.
  private readonly staleThresholdMs: number;
  private lastSuccessAt: Date | null = null;
  private lastSuccessObservationId: string | null = null;
  private startedAt: Date = new Date();
  private startupComplete = false;
  private readonessProvenance: Map<string, DependencyProvenance> =
    new Map();
  private readinessReasons: string[] = [];

  constructor(@Optional() private configService?: ConfigService) {
    this.maxBlocksInMemory =
      this.configService?.get<number>('blockchain.maxBlocksInMemory', 10000) ??
      10000;
    this.maxEventsInMemory =
      this.configService?.get<number>('blockchain.maxEventsInMemory', 50000) ??
      50000;
    this.maxReorgHistoryEntries =
      this.configService?.get<number>(
        'blockchain.maxReorgHistoryEntries',
        1000,
      ) ?? 1000;

    this.indexerLagThresholdBlocks =
      this.configService?.get<number>(
        'blockchain.indexerLagThresholdBlocks',
        150,
      ) ?? 150;
    this.finalityLagThresholdBlocks =
      this.configService?.get<number>(
        'blockchain.finalityLagThresholdBlocks',
        this.configService?.get<number>(
          'blockchain.projectionLagThresholdBlocks',
          150,
        ) ?? 150,
      ) ?? 150;
    for (const [name, threshold] of [
      ['blockchain.indexerLagThresholdBlocks', this.indexerLagThresholdBlocks],
      [
        'blockchain.finalityLagThresholdBlocks',
        this.finalityLagThresholdBlocks,
      ],
    ] as const) {
      if (!Number.isSafeInteger(threshold) || threshold < 0) {
        throw new Error(`${name} must be a non-negative safe integer`);
      }
    }
    this.rpcFailureWindow =
      this.configService?.get<number>(
        'blockchain.rpcFailureWindowMs',
        300000,
      ) ?? 300000;
    this.rpcFailureLimit =
      this.configService?.get<number>('blockchain.rpcFailureLimit', 20) ?? 20;
    this.maxDeadLetters =
      this.configService?.get<number>('blockchain.maxDeadLetters', 100) ?? 100;
    this.runbookUrl =
      this.configService?.get<string>(
        'blockchain.indexerRunbookUrl',
        'https://github.com/DigiNodes/truthbounty-api/blob/main/docs/indexer-runbook.md',
      ) ??
      'https://github.com/DigiNodes/truthbounty-api/blob/main/docs/indexer-runbook.md';
    this.staleThresholdMs =
      this.configService?.get<number>(
        'blockchain.readinessStaleThresholdMs',
        DEFAULT_STANE_THRESHOLD_MS,
      ) ?? DEFAULT_STANE_THRESHOLD_MS;
    if (!Number.isFinite(this.staleThresholdMs) || this.staleThresholdMs <= 0) {
      throw new Error(
        'blockchain.readinessStaleThresholdMs must be a positive number',
      );
    }

    this.logger.log(
      `Memory limits — blocks: ${this.maxBlocksInMemory}, ` +
        `events: ${this.maxEventsInMemory}, ` +
        `reorg history: ${this.maxReorgHistoryEntries}, ` +
        `indexer lag threshold: ${this.indexerLagThresholdBlocks} blocks, ` +
        `finality lag threshold: ${this.finalityLagThresholdBlocks} blocks, ` +
        `readiness stale threshold: ${this.staleThresholdMs}ms`,
    );
  }

  async saveBlock(block: BlockInfo): Promise<BlockRecord> {
    const blockRecord: BlockRecord = {
      id: `${block.number}:${block.hash}`,
      blockNumber: block.number,
      blockHash: block.hash,
      parentHash: block.parentHash,
      timestamp: block.timestamp,
      isCanonical: true,
      createdAt: new Date(),
    };

    this.blocks.set(blockRecord.id, blockRecord);
    this.evictOldBlocks();
    return blockRecord;
  }

  async getBlock(
    blockNumber: number,
    blockHash: string,
  ): Promise<BlockRecord | null> {
    const record = this.blocks.get(`${blockNumber}:${blockHash}`);
    return record || null;
  }

  async getBlocksAtHeight(blockNumber: number): Promise<BlockRecord[]> {
    const result: BlockRecord[] = [];
    this.blocks.forEach((block) => {
      if (block.blockNumber === blockNumber) {
        result.push(block);
      }
    });
    return result;
  }

  async getCanonicalBlock(blockNumber: number): Promise<BlockRecord | null> {
    const blocks = await this.getBlocksAtHeight(blockNumber);
    return blocks.find((b) => b.isCanonical) || null;
  }

  async getCanonicalBlockByHash(
    blockHash: string,
  ): Promise<BlockRecord | null> {
    for (const [, block] of this.blocks) {
      if (block.blockHash === blockHash && block.isCanonical) {
        return block;
      }
    }
    return null;
  }

  async savePendingEvent(event: PendingEvent): Promise<void> {
    this.events.set(event.id, event);
    if (event.status === 'pending') {
      this.chainState.pendingEventCount++;
    }
    this.evictOldEvents();
  }

  async getEvent(eventId: string): Promise<PendingEvent | null> {
    return this.events.get(eventId) || null;
  }

  async getEventsByBlock(blockNumber: number): Promise<PendingEvent[]> {
    const blockEvents: PendingEvent[] = [];
    this.events.forEach((event) => {
      if (event.blockNumber === blockNumber) {
        blockEvents.push(event);
      }
    });
    return blockEvents;
  }

  async getPendingEvents(): Promise<PendingEvent[]> {
    const pending: PendingEvent[] = [];
    this.events.forEach((event) => {
      if (event.status === 'pending') {
        pending.push(event);
      }
    });
    return pending;
  }

  async getOrphanedEvents(): Promise<PendingEvent[]> {
    const orphaned: PendingEvent[] = [];
    this.events.forEach((event) => {
      if (event.status === 'orphaned') {
        orphaned.push(event);
      }
    });
    return orphaned;
  }

  async updateEventStatus(
    eventId: string,
    status: 'pending' | 'confirmed' | 'orphaned',
    confirmations?: number,
  ): Promise<void> {
    const event = this.events.get(eventId);
    if (!event) {
      throw new Error(`Event not found: ${eventId}`);
    }

    const oldStatus = event.status;
    event.status = status;
    event.confirmations = confirmations ?? event.confirmations;

    if (status === 'confirmed') {
      event.confirmedAt = new Date();
      if (oldStatus === 'pending') {
        this.chainState.pendingEventCount--;
      }
    } else if (status === 'orphaned') {
      if (oldStatus === 'pending') {
        this.chainState.pendingEventCount--;
      }
      this.chainState.orphanedEventCount++;
    }
  }

  async markBlocksNonCanonical(blockNumbers: number[]): Promise<void> {
    this.blocks.forEach((block) => {
      if (blockNumbers.includes(block.blockNumber)) {
        block.isCanonical = false;
      }
    });
  }

  async recordReorg(reorg: ReorgEvent): Promise<void> {
    this.reorgHistory.push(reorg);
    this.chainState.lastReorgTime = reorg.detectedAt;
    this.trimReorgHistory();
  }

  async getReorgHistory(): Promise<ReorgEvent[]> {
    return this.reorgHistory;
  }

  async updateChainState(partial: Partial<ChainState>): Promise<void> {
    this.chainState = { ...this.chainState, ...partial };
  }

  async getChainState(): Promise<ChainState> {
    return { ...this.chainState };
  }

  /**
   * Record the highest block observed from the RPC provider (the observed head).
   * Updates the derived projection lag against the finalized cursor.
   */
  async setObservedHead(blockNumber: number): Promise<void> {
    this.chainState.observedHeadBlock = blockNumber;
    this.chainState.lastObservedAt = new Date().toISOString();
    this.refreshProjectionLag();
  }

  /**
   * Record the chain's safe cursor (reorg-unlikely boundary).
   */
  async setSafeBlock(blockNumber: number): Promise<void> {
    this.chainState.safeBlock = blockNumber;
  }

  /**
   * Record the chain's finalized cursor (finality boundary).
   * Recomputes projection lag against the observed head.
   */
  async setFinalizedBlock(blockNumber: number): Promise<void> {
    this.chainState.finalizedBlock = blockNumber;
    this.chainState.lastFinalizedAt = new Date().toISOString();
    this.refreshProjectionLag();
  }

  /**
   * Record the highest block to which projections (derived, rebuildable state)
   * have advanced.
   */
  async setProjectionHead(blockNumber: number): Promise<void> {
    this.chainState.projectionHeadBlock = blockNumber;
    this.chainState.lastProjectionAt = new Date().toISOString();
  }

  /**
   * Record a single RPC failure. Failure rate is evaluated over a sliding window
   * (default 5 minutes). The monotonic counter is preserved for Prometheus.
   */
  async recordRpcFailure(error?: Error | string): Promise<void> {
    const now = Date.now();
    this.chainState.rpcFailureCount =
      (this.chainState.rpcFailureCount ?? 0) + 1;
    this.chainState.lastRpcErrorAt = new Date(now).toISOString();
    this.rpcFailureTimestamps.push(now);

    // Keep only failures within the sliding window.
    while (
      this.rpcFailureTimestamps.length > 0 &&
      this.rpcFailureTimestamps[0] < now - this.rpcFailureWindow
    ) {
      this.rpcFailureTimestamps.shift();
    }

    const message =
      error instanceof Error ? error.message : String(error ?? '');
    this.logger.warn(
      `RPC failure recorded (total=${this.chainState.rpcFailureCount}, ` +
        `window=${this.rpcFailureTimestamps.length}${message ? `): ${message}` : ')'}`,
    );
  }

  /**
   * Record an event replay (reprocessed after a reorg or retry).
   */
  async recordReplay(count = 1): Promise<void> {
    this.chainState.replayCount = (this.chainState.replayCount ?? 0) + count;
  }

  /**
   * Record a dead-lettered event (one that failed past max retries).
   */
  async recordDeadLetter(count = 1): Promise<void> {
    this.chainState.deadLetterCount =
      (this.chainState.deadLetterCount ?? 0) + count;
  }

  /**
   * Number of RPC failures within the configured sliding window.
   */
  getRpcFailuresInWindow(): number {
    const now = Date.now();
    return this.rpcFailureTimestamps.filter(
      (t) => t >= now - this.rpcFailureWindow,
    ).length;
  }

  /**
   * Projection lag in blocks, computed from the finalized cursor to the observed head.
   * Returns 0 when no head has been observed yet.
   */
  getProjectionLag(): number {
    const lag =
      (this.chainState.observedHeadBlock ?? 0) -
      (this.chainState.finalizedBlock ?? 0);
    return lag > 0 ? lag : 0;
  }

  getIndexerLag(): number {
    const lag =
      (this.chainState.observedHeadBlock ?? 0) -
      (this.chainState.lastProcessedBlock ?? 0);
    return lag > 0 ? lag : 0;
  }

  private refreshProjectionLag(): void {
    this.chainState.projectionLag = this.getProjectionLag();
  }

  /**
   * Record a successful observation. Readiness requires a fresh successful
   * observation; this is the only way to clear a stale or degraded state.
   */
  async recordSuccessfulObservation(observationId?: string): Promise<void> {
    this.lastSuccessAt = new Date();
    this.lastSuccessObservationId = observationId ?? null;
  }

  /**
   * Mark startup as complete. Until this is called, readiness is withheld.
   */
  async markStartupComplete(): Promise<void> {
    this.startupComplete = true;
  }

  /**
   * Upsert provenance for a single dependency (rpc, redis, db, indexer, ipfs).
   */
  async setDependencyProvenance(
    name: string,
    provenance: DependencyProvenance,
  ): Promise<void> {
    this.readonessProvenance.set(name, { ...provenance });
  }

  /**
   * Record a dependency failure. A required dependency failure removes readiness.
   */
  async recordDependencyFailure(
    name: string,
    reason: string,
    required = true,
  ): Promise<void> {
    const existing = this.readonessProvenance.get(name);
    this.readonessProvenance.set(name, {
      name,
      ok: false,
      required,
      observedAt: new Date().toISOString(),
      lastSuccessAt: existing?.lastSuccessAt ?? null,
      reason,
      chainId: existing?.chainId ?? null,
      releaseId: existing?.releaseId ?? null,
      deploymentBlock: existing?.deploymentBlock ?? null,
      artifactChecksum: existing?.artifactChecksum ?? null,
    });
  }

  /**
   * Compute the current readiness snapshot. Fails closed on stale, missing, or
   * failed required dependencies. Public status is sanitized (reasons are
   * codes, not internal details).
   */
  getReadinessSnapshot(): ReadinessSnapshot {
    const now = Date.now();
    const reasons: string[] = [];

    if (!this.startupComplete) {
      reasons.push('startup_incomplete');
    }

    const lastSuccessAt = this.lastSuccessAt;
    if (!lastSuccessAt) {
      reasons.push('no_successful_observation');
    } else if (now - lastSuccessAt.getTime() > this.staleThresholdMs) {
      reasons.push('stale_success');
    }

    const projectionLag = this.getProjectionLag();
    if (projectionLag > this.finalityLagThresholdBlocks) {
      reasons.push('rpc_lag');
    }

    const indexerLag = this.getIndexerLag();
    if (indexerLag > this.indexerLagThresholdBlocks) {
      reasons.push('indexer_gap');
    }

    if (this.getRpcFailuresInWindow() > this.rpcFailureLimit) {
      reasons.push('rpc_failure_rate');
    }

    if ((this.chainState.deadLetterCount ?? 0) > this.maxDeadLetters) {
      reasons.push('dead_letter_overflow');
    }

    const provenance: DependencyProvenance[] = [];
    this.readonessProvenance.forEach((p) => {
      provenance.push({ ...p });
      if (p.required && !p.ok) {
        reasons.push(`dependency_failed:${p.name}`);
      }
    });

    this.readinessReasons = reasons;

    let status: ReadinessStatus;
    if (!this.startupComplete) {
      status = 'starting';
    } else if (reasons.length > 0) {
      status = 'degraded';
    } else {
      status = 'healthy';
    }

    return {
      status,
      observedAt: new Date(now).toISOString(),
      lastSuccessAt: lastSuccessAt
        ? lastSuccessAt.toISOString()
        : null,
      chainHead: this.chainState.observedHeadBlock ?? null,
      finalizedBlock: this.chainState.finalizedBlock ?? null,
      projectionLag,
      releaseId: null,
      reasons,
      provenance,
    };
  }

  /**
   * Sanitized public status — no internal details or provenance leaked.
   */
  getPublicStatus(): { status: ReadinessStatus; reasons: string[] } {
    const snapshot = this.getReadinessSnapshot();
    return {
      status: snapshot.status,
      reasons: snapshot.reasons.map((r) =>
        r.startsWith('dependency_failed') ? 'dependency_failed' : r,
      ),
    };
  }

  getHealthSnapshot(): IndexerHealthSnapshot {
    const readiness = this.getReadinessSnapshot();
    const status: IndexerHealthStatus =
      readiness.status === 'healthy' ? 'healthy' : 'degraded';
    return {
      status,
      readiness,
      runbookUrl: this.runbookUrl,
    };
  }

  getMemoryStats(): StateMemoryStats {
    return {
      blocksInMemory: this.blocks.size,
      eventsInMemory: this.events.size,
      reorgHistoryEntries: this.reorgHistory.length,
      maxBlocksInMemory: this.maxBlocksInMemory,
      maxEventsInMemory: this.maxEventsInMemory,
      maxReorgHistoryEntries: this.maxReorgHistoryEntries,
    };
  }

  private evictOldBlocks(): void {
    while (this.blocks.size > this.maxBlocksInMemory) {
      const oldestKey = this.blocks.keys().next().value;
      if (oldestKey === undefined) {
        break;
      }
      this.blocks.delete(oldestKey);
    }
  }

  private evictOldEvents(): void {
    while (this.events.size > this.maxEventsInMemory) {
      const oldestKey = this.events.keys().next().value;
      if (oldestKey === undefined) {
        break;
      }
      this.events.delete(oldestKey);
    }
  }

  private trimReorgHistory(): void {
    while (this.reorgHistory.length > this.maxReorgHistoryEntries) {
      this.reorgHistory.shift();
    }
  }
}
