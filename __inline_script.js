
async function api(url, payload) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload || {}),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.detail || ('HTTP ' + res.status));
  return data;
}

function setNote(text, kind = '') {
  const node = document.getElementById('loginNote');
  node.textContent = text || '';
  node.className = 'note' + (kind ? ' ' + kind : '');
}

function showVerifyStep(email) {
  document.getElementById('stepEmail').classList.remove('active');
  document.getElementById('stepVerify').classList.add('active');
  document.getElementById('verify_email').value = email;
  document.getElementById('verify_code').focus();
}

async function requestCode(force = false) {
  const emailInput = force ? document.getElementById('verify_email') : document.getElementById('login_email');
  const email = String(emailInput.value || '').trim();
  if (!email) {
    setNote('Vui lòng nhập email trước', 'error');
    return;
  }
  const button = document.getElementById(force ? 'resendBtn' : 'requestBtn');
  button.disabled = true;
  setNote('Đang gửi mã xác nhận...');
  try {
    const out = await api('/api/auth/request-code', { email });
    showVerifyStep(out.email || email);
    setNote(out.message || 'Đã gửi mã xác nhận vào mail của bạn', 'ok');
  } catch (e) {
    setNote(e.message, 'error');
  } finally {
    button.disabled = false;
  }
}

async function verifyCode() {
  const email = String(document.getElementById('verify_email').value || '').trim();
  const code = String(document.getElementById('verify_code').value || '').trim();
  const button = document.getElementById('verifyBtn');
  button.disabled = true;
  setNote('Đang xác nhận mã...');
  try {
    await api('/api/auth/verify-code', { email, code });
    window.location.href = '/';
  } catch (e) {
    setNote(e.message, 'error');
  } finally {
    button.disabled = false;
  }
}

document.getElementById('login_email').addEventListener('keydown', e => {
  if (e.key === 'Enter') requestCode();
});
document.getElementById('verify_code').addEventListener('keydown', e => {
  if (e.key === 'Enter') verifyCode();
});
