#!/bin/bash
# Keeps the two pipeline jobs alive. The TaskManager can OOM on metaspace after
# long uptime; Docker restarts the container, but the Flink jobs end up FAILED and
# would otherwise stay down. This resubmits whichever job is missing.
#
# Rebuild-aware: it matches the stage-2 job by name PREFIX, so while a rebuild is
# running its shadow job (pageledger-stage2-fold[page_state_shadow]) still counts
# as alive and the watchdog won't submit a competing job. A job must also be
# missing on two consecutive checks before resubmitting, so it can't race the
# brief cancel/submit window inside a rebuild.

JM="${FLINK_JM_ADDRESS:-flink-jobmanager:8081}"
INTERVAL="${WATCHDOG_INTERVAL:-30}"
miss1=0
miss2=0

counts() {
  curl -sf "http://${JM}/jobs/overview" 2>/dev/null | python3 -c "
import sys, json
try:
    jobs = json.load(sys.stdin).get('jobs', [])
except Exception:
    print('x x'); raise SystemExit
dead = ('CANCELED', 'FAILED', 'FINISHED')
alive = lambda p: sum(1 for j in jobs if j['name'].startswith(p) and j['state'] not in dead)
print(alive('pageledger-stage1-validate'), alive('pageledger-stage2-fold'))
"
}

echo "[watchdog] watching ${JM} every ${INTERVAL}s"
while true; do
  read -r s1 s2 <<<"$(counts)"

  if [ "$s1" = "0" ]; then miss1=$((miss1 + 1)); else miss1=0; fi
  if [ "$s2" = "0" ]; then miss2=$((miss2 + 1)); else miss2=0; fi

  if [ "$miss1" -ge 2 ]; then
    echo "[watchdog] stage 1 missing — resubmitting"
    flink run -d -m "$JM" -py /opt/flink/usrlib/stage1_validate.py || true
    miss1=0
  fi
  if [ "$miss2" -ge 2 ]; then
    echo "[watchdog] stage 2 missing — resubmitting"
    flink run -d -m "$JM" -py /opt/flink/usrlib/stage2_fold.py --sink-table page_state || true
    miss2=0
  fi

  sleep "$INTERVAL"
done
