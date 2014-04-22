select
    sum(if(event_beta = 'on', 1, 0)) as 'Beta opt in',
    sum(if(event_beta = 'off', 1, 0)) as 'Beta opt out',
    sum(if(event_alpha = 'on', 1, 0)) as 'Alpha opt in',
    sum(if(event_alpha = 'off', 1, 0)) as 'Alpha opt out',
    sum(if(event_images = 'on', 1, 0)) as 'Images turned on',
    sum(if(event_images = 'off', 1, 0)) as 'Images turned off'
from 
    MobileOptionsTracking_8101982
where
    timestamp >= '{from_timestamp}' and
    timestamp <= '{to_timestamp}'
