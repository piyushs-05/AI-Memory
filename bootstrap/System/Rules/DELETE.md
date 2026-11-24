---
policy_version: "1"
protected: true
---
# Rule: DELETE

1. **Archive (`archive_record`)** is reversible removal from routine active retrieval and dashboards.
2. **Forget (`forget_record`)** is an explicit deletion operation that removes active copies, clears derived indexes/caches, and records a minimal content-free tombstone to prevent ordinary reimport.
3. Forgetting does not retroactively rewrite offline backups or external provider logs; those limitations must be reported accurately.
