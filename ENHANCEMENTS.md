# Recent Enhancements

- Hardened Firestore lookups: ignore empty story/goal/sprint IDs to avoid `document("")` crashes.
- Logging improvements: include task title in created-task sync log and clearer permission-denied warning for ref lookups.
- Completion TTL: when a reminder is newer and completed, set `completedAt` and `deleteAfter` (30 days) and clear them when reopened.
- Client cleanup: on sync, remove local reminders whose `deleteAfter` has passed and clear their Firestore reminder mapping.
- Dedupe coverage: local dedupe sweep plus server Functions (`deduplicateTasks`, `cleanupDuplicateTasksNow`) invoked every sync; duplicates mark `duplicateOf` and get TTL fields.
- Note repair: normalized story/goal refs in reminder metadata rebuilds to prevent empty IDs and keep tags/list info consistent.
