select
	COALESCE(sum(case when ThankYous.event_eventSource = 'history' then 1 else 0 end), 0) as desktopHistory,
	COALESCE(sum(case when ThankYous.event_eventSource = 'diff' then 1 else 0 end), 0) as desktopDiffThanks,
	COALESCE(sum(case when ThankYous.event_eventSource = 'mobilediff' then 1 else 0 end), 0) as mobileDiffThanks
from (
	select
	  event_eventSource
	from Echo_7731316
	where
	  event_notificationType = 'edit-thank'
	  and timestamp >= '{from_timestamp}'
	  and timestamp <= '{to_timestamp}'
) as ThankYous
