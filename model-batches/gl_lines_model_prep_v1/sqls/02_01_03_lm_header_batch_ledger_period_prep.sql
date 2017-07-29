select
    jhb.*,
    gpc.*
from
    ${gl_header_interim_cache} jhb,
    ${gl_periods_cache} gpc
where
    jhb.batch_period_set_name=gpc.period_period_set_name
    and jhb.head_period_name=gpc.period_period_name
