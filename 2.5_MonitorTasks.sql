-- This script shows what external scripts are executing, and also lets us view the worker thread IDs for those tasks
SELECT * 
FROM sys.dm_external_script_requests
GO

SELECT R.Session_Id,
Th.os_thread_id
FROM   sys.dm_exec_requests AS R
INNER JOIN
sys.dm_exec_sessions AS S
ON R.session_id = S.session_id
INNER JOIN
sys.dm_os_tasks AS T
ON R.session_id = T.session_id
INNER JOIN
sys.dm_os_workers AS W
ON T.worker_address = W.worker_address
INNER JOIN
sys.dm_os_threads AS Th
ON W.thread_address = Th.thread_address
WHERE  S.is_user_process = 1;
GO