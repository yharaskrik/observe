import { Inject, Injectable, Logger } from "@nestjs/common";
import { AsyncLocalStorage } from "async_hooks";
import { ObserveAgentSharedBuffer } from "../agent/observe-agent.shared-buffer.js";
import {
  JobSnapshot,
  ObserveModuleOptionsWithDefaults,
} from "../interfaces/index.js";
import { OBSERVE_OPTIONS } from "../observe.constants.js";
import { OperationTraceRegistry } from "../services/operation-trace.registry.js";
import { KeyOf } from "../types/key-of.type.js";
import {
  describePeerLoadError,
  loadOptionalPeer,
} from "../utils/optional-peer.util.js";
import { JobRunDescriptor, JobTraceRunner } from "./job-trace-runner.js";

/**
 * The fields of a pg-boss job this agent reads, structurally typed. The dates
 * are only there when the worker asked for `includeMetadata`.
 */
interface PgBossJobLike {
  id?: string;
  name?: string;
  retryCount?: number;
  retryLimit?: number;
  createdOn?: Date | string;
  startAfter?: Date | string;
  startedOn?: Date | string;
}

type PgBossWorkHandler = (jobs: PgBossJobLike[]) => unknown;

interface PgBossPrototype {
  work?: (name: string, ...args: unknown[]) => unknown;
  [key: symbol]: unknown;
}

interface PgBossModule {
  PgBoss?: { prototype?: PgBossPrototype };
  default?: { prototype?: PgBossPrototype };
}

const ORIGINAL_WORK = Symbol.for("nestjs.observe.pgBoss.work");

function toTime(value: Date | string | undefined): number | undefined {
  if (value === undefined) {
    return undefined;
  }
  const time = value instanceof Date ? value.getTime() : Date.parse(value);
  return Number.isNaN(time) ? undefined : time;
}

/**
 * Job tracing for pg-boss, a Postgres-backed queue with no Nest wrapper of its
 * own - applications call `boss.work(queue, handler)` from wherever they like,
 * so the client's `work` is patched rather than any explorer.
 *
 * pg-boss hands a handler an array of jobs. A batch of one - the default -
 * reports as that job; a larger batch is a single handler call, so it reports
 * as one run of the queue, without a job id.
 *
 * Enqueueing is not patched: pg-boss persists only the options it knows, so an
 * id stamped there would never reach the worker, and the job's `data` belongs
 * to the application. Each run therefore mints its own trace id.
 */
@Injectable()
export class PgBossObserveAgentService<Store extends Record<string, unknown>> {
  private readonly logger = new Logger(PgBossObserveAgentService.name);
  private readonly runner: JobTraceRunner<Store>;

  constructor(
    observeAgentSharedBuffer: ObserveAgentSharedBuffer,
    @Inject(OBSERVE_OPTIONS)
    options: ObserveModuleOptionsWithDefaults,
    operationTraceRegistry: OperationTraceRegistry,
    asyncLocalStorage: AsyncLocalStorage<Map<KeyOf<Store>, any>>,
  ) {
    this.runner = new JobTraceRunner(
      observeAgentSharedBuffer,
      options,
      operationTraceRegistry,
      asyncLocalStorage,
      this.logger,
    );
    this.patchPgBoss();
  }

  private loadPgBossPrototype(): PgBossPrototype | null | undefined {
    const result = loadOptionalPeer<PgBossModule>("pg-boss");
    if (!result.installed) {
      return undefined;
    }
    if (result.error) {
      this.logger.warn(
        `pg-boss is installed but could not be loaded, so its jobs will not be instrumented: ${describePeerLoadError(result.error)}`,
      );
      return null;
    }
    // v12 exports the class by name; earlier majors as the default export.
    return (
      result.module?.PgBoss?.prototype ??
      result.module?.default?.prototype ??
      null
    );
  }

  private readQueueMetadata(job: PgBossJobLike): Partial<JobSnapshot> {
    const metadata: Partial<JobSnapshot> = {};

    const createdOn = toTime(job.createdOn);
    if (createdOn !== undefined) {
      metadata.enqueuedAt = new Date(createdOn).toISOString();
      const startedOn = toTime(job.startedOn) ?? Date.now();
      // As with BullMQ: a `startAfter` the caller asked for is a schedule,
      // not a backlog, so the wait is measured from when the job became
      // runnable.
      const runnableAt = Math.max(createdOn, toTime(job.startAfter) ?? 0);
      metadata.waitDuration = Math.max(0, startedOn - runnableAt);
    }
    if (typeof job.retryCount === "number") {
      metadata.attemptsMade = job.retryCount;
    }
    if (typeof job.retryLimit === "number") {
      // The limit counts retries; the first attempt is not one of them.
      metadata.maxAttempts = job.retryLimit + 1;
    }
    return metadata;
  }

  private describe(queueName: string, jobs: unknown): JobRunDescriptor {
    const batch = Array.isArray(jobs) ? (jobs as PgBossJobLike[]) : [];
    const only = batch.length === 1 ? batch[0] : undefined;
    return {
      queueName,
      name: queueName,
      id: only?.id,
      metadata: only ? this.readQueueMetadata(only) : {},
    };
  }

  private patchPgBoss() {
    const prototype = this.loadPgBossPrototype();
    if (prototype === undefined) {
      // An optional peer. No pg-boss means no worker to wrap.
      return;
    }
    const originalWork = (prototype?.[ORIGINAL_WORK] ??
      prototype?.work) as PgBossPrototype["work"];
    if (!prototype || typeof originalWork !== "function") {
      this.logger.warn(
        "pg-boss's PgBoss.prototype.work is not available, so its jobs will not be instrumented.",
      );
      return;
    }
    prototype[ORIGINAL_WORK] = originalWork;

    const instrument = (queueName: string, handler: PgBossWorkHandler) =>
      this.instrument(queueName, handler);
    // work(name, handler) | work(name, options, handler)
    prototype.work = function (
      this: unknown,
      name: string,
      ...args: unknown[]
    ) {
      const last = args.length - 1;
      if (typeof name === "string" && typeof args[last] === "function") {
        args[last] = instrument(name, args[last] as PgBossWorkHandler);
      }
      return originalWork.call(this, name, ...args);
    };
  }

  private instrument(
    queueName: string,
    handler: PgBossWorkHandler,
  ): PgBossWorkHandler {
    const runner = this.runner;
    const describe = (jobs: unknown) => this.describe(queueName, jobs);
    return function (this: unknown, jobs: PgBossJobLike[]) {
      return runner.run(describe(jobs), () => handler.call(this, jobs));
    };
  }
}
