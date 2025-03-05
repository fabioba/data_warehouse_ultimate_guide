update sales.CFG_FLOW_MANAGER
set LAST_RUN = current_timestamp
where PROCESS = {param.PROCESS}