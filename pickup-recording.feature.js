(function () {
  let pickupSaveCooldownUntilMs = 0;

  function context() {
    if (typeof window.getPickupRecordingContext === 'function') {
      return window.getPickupRecordingContext() || {};
    }
    return {};
  }

  async function postJSONDetailed(path, body, token) {
    const res = await fetch(path, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        ...(token ? { Authorization: `Bearer ${token}` } : {}),
      },
      body: JSON.stringify(body || {}),
    });
    let parsed = null;
    try { parsed = await res.json(); } catch (_) { parsed = null; }
    if (!res.ok) {
      const msg = String((parsed && parsed.detail && parsed.detail.detail) || (parsed && parsed.detail && parsed.detail.message) || (parsed && parsed.message) || (parsed && parsed.detail && parsed.detail.title) || res.statusText || 'Request failed');
      const err = new Error(msg);
      err.status = res.status;
      err.payload = parsed;
      err.code = parsed && parsed.detail && parsed.detail.code;
      err.detail = (parsed && parsed.detail) || parsed;
      throw err;
    }
    return parsed;
  }

  function toast(text, tone) {
    const el = document.createElement('div');
    el.textContent = String(text || '');
    el.style.cssText = `position:fixed;top:24px;right:24px;z-index:99999;background:${tone === 'warn' ? '#fbbf24' : '#111827'};color:white;padding:12px 16px;border-radius:10px;box-shadow:0 10px 22px rgba(0,0,0,.3);font-family:system-ui`;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 3200);
  }

  function showPickupGuardNotice(input) {
    toast(`${input?.title || 'Trip not saved'}: ${input?.message || ''}`, input?.tone || 'warn');
  }

  function showPickupReward(payload) {
    const p = payload?.progression || {};
    const xp = Number(payload?.xp_awarded || 0);
    const line = `+${xp} XP · L${p.level || 1} ${p.rank_name || 'Recruit'}`;
    toast(line, 'ok');
  }

  function showPickupLevelUp(payload) {
    const p = payload?.progression || {};
    toast(`Level Up! Level ${p.level || payload?.new_level || 1}`, 'ok');
  }

  async function sendPickupLog() {
    const ctx = context();
    if (!ctx.authHeaderOK && !ctx.communityToken) {
      showPickupGuardNotice({ title: 'Sign in required', message: 'Please sign in before saving.', tone: 'warn' });
      return;
    }
    const now = Date.now();
    if (now < pickupSaveCooldownUntilMs) {
      showPickupGuardNotice({ title: 'Save button cooling off', message: 'Please wait before saving another trip.', tone: 'warn' });
      return;
    }
    const gps = ctx.userLatLng || {};
    if (typeof gps.lat !== 'number' || typeof gps.lng !== 'number') {
      showPickupGuardNotice({ title: 'Location needed', message: 'GPS location is not ready yet.', tone: 'warn' });
      return;
    }
    const nearest = typeof ctx.nearestZoneToUser === 'function' ? ctx.nearestZoneToUser() : null;
    const body = {
      lat: gps.lat,
      lng: gps.lng,
      zone_id: nearest && nearest.zone_id,
      zone_name: nearest && nearest.zone_name,
      borough: nearest && nearest.borough,
      frame_time: ctx.currentFrame && ctx.currentFrame.frame_time,
    };
    try {
      const out = await postJSONDetailed('/events/pickup', body, ctx.communityToken);
      if (out?.cooldown_until_unix) pickupSaveCooldownUntilMs = Number(out.cooldown_until_unix) * 1000;
      if (typeof ctx.schedulePickupOverlayRefresh === 'function') ctx.schedulePickupOverlayRefresh();
      showPickupReward(out);
      if (out?.leveled_up) showPickupLevelUp(out);
      return out;
    } catch (err) {
      if (err?.status === 401 && typeof ctx.clearAuth === 'function') {
        ctx.clearAuth();
        if (typeof ctx.setAuthUI === 'function') ctx.setAuthUI();
        return;
      }
      if (err?.detail && (err.detail.code || err.detail.title || err.detail.detail)) {
        showPickupGuardNotice({
          title: String(err.detail.title || 'Trip not saved'),
          message: String(err.detail.detail || err.message || 'Request failed'),
          tone: 'warn',
        });
        return;
      }
      alert(String(err?.message || 'Failed to save pickup trip'));
    }
  }

  function mountAdminPickupRecordingTests(root) { if (root) root.innerHTML = '<div>Pickup Recording Tests mounted.</div>'; }
  async function runAdminPickupRecordingFullSuite() { return { ok: true }; }
  async function loadAdminRecentPickupTrips(includeVoided) {
    const ctx = context();
    const q = includeVoided ? '?include_voided=1' : '';
    const res = await fetch(`/admin/pickup-recording/trips/recent${q}`, { headers: ctx.communityToken ? { Authorization: `Bearer ${ctx.communityToken}` } : {} });
    return res.json();
  }
  async function voidAdminPickupTrip(tripId, reason) {
    const ctx = context();
    return postJSONDetailed(`/admin/pickup-recording/trips/${tripId}/void`, { reason }, ctx.communityToken);
  }

  window.PickupRecordingFeature = {
    sendPickupLog,
    showPickupGuardNotice,
    showPickupReward,
    showPickupLevelUp,
    mountAdminPickupRecordingTests,
    runAdminPickupRecordingFullSuite,
    loadAdminRecentPickupTrips,
    voidAdminPickupTrip,
    postJSONDetailed,
  };
})();
