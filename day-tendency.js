(() => {
  const DAY_TENDENCY_REFRESH_MS = 30 * 60 * 1000;
  const DAY_TENDENCY_RETRY_MS = 10 * 1000;

  const STATE = {
    root: null,
    marker: null,
    score: null,
    band: null,
    refreshTimer: null,
    retryTimer: null,
    resizeObserver: null,
    mutationObserver: null,
    positionPollTimer: null,
    isRefreshing: false,
  };

  function apiBase() {
    return String(window.API_BASE || '').replace(/\/$/, '');
  }

  async function fetchJSONWithTimeout(url, timeoutMs = 10000) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, {
        method: 'GET',
        headers: { Accept: 'application/json' },
        signal: controller.signal,
      });
      if (!res.ok) {
        throw new Error(`HTTP ${res.status}`);
      }
      return await res.json();
    } finally {
      clearTimeout(timeout);
    }
  }

  function ensureDayTendencyStyles() {
    if (document.getElementById('dayTendencyStyles')) return;

    const style = document.createElement('style');
    style.id = 'dayTendencyStyles';
    style.textContent = `
      .dayTendencyMeter {
        position: fixed;
        top: 56px;
        left: 12px;
        width: 76px;
        padding: 8px 8px 9px;
        border-radius: 12px;
        border: 1px solid rgba(0, 0, 0, 0.14);
        background: rgba(255, 255, 255, 0.84);
        box-shadow: 0 4px 12px rgba(0, 0, 0, 0.14);
        backdrop-filter: blur(2px);
        -webkit-backdrop-filter: blur(2px);
        z-index: 1399;
        color: #111;
        font-family: Inter, system-ui, -apple-system, Segoe UI, Roboto, sans-serif;
        user-select: none;
      }
      .dayTendencyTitle {
        font-size: 11px;
        line-height: 1.1;
        font-weight: 800;
        letter-spacing: 0.01em;
      }
      .dayTendencySub {
        margin-top: 2px;
        font-size: 10px;
        line-height: 1.1;
        opacity: 0.78;
        font-weight: 600;
      }
      .dayTendencyBarWrap {
        margin-top: 6px;
        display: flex;
        align-items: flex-end;
        gap: 7px;
      }
      .dayTendencyScale {
        position: relative;
        width: 16px;
        height: 120px;
        border-radius: 10px;
        border: 1px solid rgba(0, 0, 0, 0.17);
        background: linear-gradient(to top, #e60000 0%, #ffd400 50%, #00b050 100%);
        overflow: hidden;
      }
      .dayTendencyMarker {
        position: absolute;
        left: -1px;
        width: calc(100% + 2px);
        border-top: 2px solid #fff;
        box-shadow: 0 0 2px rgba(0, 0, 0, 0.6);
        bottom: 0%;
        pointer-events: none;
      }
      .dayTendencyLabels {
        min-height: 120px;
        display: flex;
        flex-direction: column;
        justify-content: flex-end;
        align-items: flex-start;
        gap: 2px;
      }
      .dayTendencyScore {
        font-size: 13px;
        font-weight: 800;
        line-height: 1;
      }
      .dayTendencyBand {
        font-size: 10px;
        line-height: 1.1;
        opacity: 0.9;
        font-weight: 700;
      }
    `;
    document.head.appendChild(style);
  }

  function ensureDayTendencyRoot() {
    if (STATE.root && document.body.contains(STATE.root)) return STATE.root;

    let root = document.getElementById('dayTendencyMeter');
    if (!root) {
      root = document.createElement('div');
      root.id = 'dayTendencyMeter';
      root.className = 'dayTendencyMeter';
      root.hidden = true;
      root.setAttribute('role', 'status');
      root.setAttribute('aria-live', 'polite');
      root.innerHTML = `
        <div class="dayTendencyTitle">Day Tendency</div>
        <div class="dayTendencySub">Expected</div>
        <div class="dayTendencyBarWrap">
          <div class="dayTendencyScale">
            <div class="dayTendencyMarker"></div>
          </div>
          <div class="dayTendencyLabels">
            <div class="dayTendencyScore">--</div>
            <div class="dayTendencyBand">--</div>
          </div>
        </div>
      `;
      document.body.appendChild(root);
    }

    STATE.root = root;
    STATE.marker = root.querySelector('.dayTendencyMarker');
    STATE.score = root.querySelector('.dayTendencyScore');
    STATE.band = root.querySelector('.dayTendencyBand');
    return root;
  }

  function positionDayTendencyRoot() {
    const root = ensureDayTendencyRoot();
    if (!root) return;

    let top = 56;
    let left = 12;
    const badge = document.getElementById('onlineBadge');

    if (badge) {
      const rect = badge.getBoundingClientRect();
      top = rect.bottom + 8;
      left = rect.left;
    }

    const sliderWrap = document.getElementById('sliderWrap');
    const dock = document.getElementById('dock');
    const meterHeight = root.offsetHeight || 180;
    const safetyBottom = 8;
    let maxTop = window.innerHeight - meterHeight - safetyBottom;

    if (sliderWrap) {
      const sliderTop = sliderWrap.getBoundingClientRect().top;
      maxTop = Math.min(maxTop, sliderTop - meterHeight - 8);
    }
    if (dock) {
      const dockTop = dock.getBoundingClientRect().top;
      maxTop = Math.min(maxTop, dockTop - meterHeight - 8);
    }

    const minTop = 8;
    if (Number.isFinite(maxTop)) {
      top = Math.min(top, maxTop);
    }
    top = Math.max(minTop, top);

    root.style.top = `${Math.round(top)}px`;
    root.style.left = `${Math.round(left)}px`;
  }

  function bandText(band) {
    const b = String(band || '').toLowerCase();
    if (b === 'high') return 'High';
    if (b === 'low') return 'Low';
    return 'Normal';
  }

  function scoreToPct(score) {
    const n = Number(score);
    if (!Number.isFinite(n)) return null;
    return Math.max(0, Math.min(100, n));
  }

  function applyDayTendencyPayload(payload) {
    const root = ensureDayTendencyRoot();
    if (!root) return false;

    if (!payload || payload.status === 'insufficient_data') {
      root.hidden = true;
      return false;
    }

    const numericScore = Number(payload.score);
    if (!Number.isFinite(numericScore)) {
      root.hidden = true;
      return false;
    }

    const scorePct = scoreToPct(numericScore);
    const pct = Number.isFinite(Number(payload?.meter_pct))
      ? Math.max(0, Math.min(100, Number(payload.meter_pct) * 100))
      : scorePct;

    const roundedScore = String(Math.round(numericScore));
    const label = payload.label || bandText(payload.band);
    const confidencePct = Number.isFinite(Number(payload?.confidence))
      ? Math.round(Number(payload.confidence) * 100)
      : 0;
    const explain = payload.explain ? ` • ${payload.explain}` : '';

    if (STATE.score) STATE.score.textContent = roundedScore;
    if (STATE.band) STATE.band.textContent = label;
    if (STATE.marker) STATE.marker.style.bottom = `${pct}%`;

    root.title = `${label} • Score ${numericScore}/100 • Confidence ${confidencePct}%${explain}`;
    root.setAttribute(
      'aria-label',
      `Day tendency expected ${String(label).toLowerCase()}, score ${roundedScore} out of 100, confidence ${confidencePct} percent`
    );
    root.hidden = false;
    positionDayTendencyRoot();
    return true;
  }

  function scheduleRetryIfNeeded(payload, hadError) {
    const status = String(payload?.status || '').toLowerCase();
    const explain = String(payload?.explain || '').toLowerCase();
    const shouldRetry = hadError || status === 'model_not_ready' || explain.includes('not ready');
    if (!shouldRetry || STATE.retryTimer) return;
    STATE.retryTimer = window.setTimeout(async () => {
      STATE.retryTimer = null;
      await refreshDayTendencyMeter();
    }, DAY_TENDENCY_RETRY_MS);
  }

  async function refreshDayTendencyMeter() {
    if (STATE.isRefreshing) return;
    STATE.isRefreshing = true;
    let payload = null;
    let hadError = false;

    try {
      const base = apiBase();
      if (!base) throw new Error('API base missing');
      payload = await fetchJSONWithTimeout(`${base}/day_tendency/today`, 10000);
      applyDayTendencyPayload(payload);
    } catch (_) {
      hadError = true;
      if (STATE.root) STATE.root.hidden = true;
    } finally {
      scheduleRetryIfNeeded(payload, hadError);
      STATE.isRefreshing = false;
    }
  }

  function startDayTendencyMeter() {
    ensureDayTendencyStyles();
    ensureDayTendencyRoot();
    positionDayTendencyRoot();

    const badge = document.getElementById('onlineBadge');

    if ('ResizeObserver' in window && badge) {
      STATE.resizeObserver = new ResizeObserver(() => positionDayTendencyRoot());
      STATE.resizeObserver.observe(badge);
    }

    if ('MutationObserver' in window && badge) {
      STATE.mutationObserver = new MutationObserver(() => positionDayTendencyRoot());
      STATE.mutationObserver.observe(badge, {
        attributes: true,
        childList: true,
        subtree: true,
      });
    }

    window.addEventListener('resize', positionDayTendencyRoot, { passive: true });
    window.addEventListener('orientationchange', positionDayTendencyRoot, { passive: true });
    window.addEventListener('scroll', positionDayTendencyRoot, { passive: true });

    document.addEventListener('visibilitychange', () => {
      if (document.visibilityState === 'visible') {
        positionDayTendencyRoot();
        refreshDayTendencyMeter();
      }
    });

    STATE.positionPollTimer = window.setInterval(positionDayTendencyRoot, 5000);
    STATE.refreshTimer = window.setInterval(refreshDayTendencyMeter, DAY_TENDENCY_REFRESH_MS);

    refreshDayTendencyMeter();
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', startDayTendencyMeter, { once: true });
  } else {
    startDayTendencyMeter();
  }
})();
