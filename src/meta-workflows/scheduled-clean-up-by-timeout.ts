import type { Db } from 'pg-boss';
import type { ScheduledCleanUpByTimeoutConfig } from '.';
import { workflow } from '../definition';
import type { WorkflowEngine } from '../engine';
import type { InputParameters, WorkflowDefinition } from '../types';

const PAGE = 100;
const DEFAULT_CONFIG = { schedule: '10m', retries: 3, maxPagesPerRun: 10 };

const selectExpiredWorkflowsByTimeoutPage = `
    SELECT id FROM workflow_runs WHERE status IN ('running', 'paused') AND timeout_at < NOW() LIMIT ${PAGE}
`;

const scheduledCleanUpByTimeout: (
  e: WorkflowEngine,
  db: Db,
  config?: ScheduledCleanUpByTimeoutConfig,
) => WorkflowDefinition<InputParameters> = (_engine, db, config) =>
  workflow(
    '__pgw-scheduled-clean-up-by-timeout',
    async ({ step, logger }) => {
      logger.log(`Scheduled clean up of expired workflows...`);

      const maxPagesPerRun = config?.maxPagesPerRun ?? DEFAULT_CONFIG.maxPagesPerRun;

      let totalCleaned = 0;
      let page = 0;
      let drained = false;

      while (page < maxPagesPerRun) {
        const cleanUpAPageResult = await step.run(`clean-page-${page}`, async () => {
          const expired = await db.executeSql(selectExpiredWorkflowsByTimeoutPage);
          const rows = expired.rows as { id: string }[];
          const ids = rows.map((r) => r.id);

          if (ids.length === 0) return { cleaned: 0 };

          const updated = await db.executeSql(
            `UPDATE workflow_runs
             SET status = 'failed', error = COALESCE(error, 'Workflow run timed out'), updated_at = NOW()
             WHERE id = ANY($1)
             RETURNING id`,
            [ids],
          );

          return { cleaned: updated.rows.length };
        });

        totalCleaned += cleanUpAPageResult.cleaned;

        if (cleanUpAPageResult.cleaned < PAGE) {
          drained = true;
          break;
        }

        page++;
      }

      return await step.run('clean-up-done', async () => {
        logger.log(
          `Clean up run finished. Cleaned ${totalCleaned}.` +
            (drained
              ? ''
              : ' Max pages per run limit hit; remaining runs will be cleaned on the next fire.'),
        );

        return { totalCleaned };
      });
    },
    {
      schedule: config?.schedule ?? DEFAULT_CONFIG.schedule,
      retries: config?.retries ?? DEFAULT_CONFIG.retries,
    },
  );

export default scheduledCleanUpByTimeout;
