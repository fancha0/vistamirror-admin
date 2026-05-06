const state = {
  inviteCode: "",
  invite: null,
  valid: false,
  status: "invalid",
};

const els = {
  message: document.getElementById("register-message"),
  form: document.getElementById("register-form"),
  codeInput: document.getElementById("invite-code-input"),
  usernameInput: document.getElementById("username-input"),
  passwordInput: document.getElementById("password-input"),
  confirmInput: document.getElementById("confirm-password-input"),
  submit: document.getElementById("register-submit"),
  codeView: document.getElementById("invite-code-view"),
  statusView: document.getElementById("invite-status-view"),
  daysView: document.getElementById("invite-days-view"),
  expiryView: document.getElementById("invite-expiry-view"),
};

function parseInviteCode() {
  const parts = window.location.pathname.split("/").filter(Boolean);
  if (parts[0] === "invite" && parts[1]) {
    return decodeURIComponent(parts[1]).trim();
  }
  const queryCode = new URLSearchParams(window.location.search).get("code");
  return (queryCode || "").trim();
}

function setMessage(type, text) {
  if (!els.message) {
    return;
  }
  els.message.className = `message ${type}`;
  els.message.textContent = text;
}

function setFormEnabled(enabled) {
  if (!els.usernameInput || !els.passwordInput || !els.confirmInput || !els.submit) {
    return;
  }
  els.usernameInput.disabled = !enabled;
  els.passwordInput.disabled = !enabled;
  els.confirmInput.disabled = !enabled;
  els.submit.disabled = !enabled;
}

function formatDate(value) {
  if (!value) {
    return "永久有效";
  }
  const parsed = new Date(value.includes("T") ? value : `${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return parsed.toLocaleString("zh-CN");
}

function statusText(status) {
  if (status === "active") return "可用";
  if (status === "used") return "已使用";
  if (status === "expired") return "已过期";
  return "无效";
}

function updateView() {
  const invite = state.invite || {};
  if (els.codeInput) {
    els.codeInput.value = state.inviteCode || "";
  }
  if (els.codeView) {
    els.codeView.textContent = invite.code || state.inviteCode || "-";
  }
  if (els.statusView) {
    els.statusView.textContent = statusText(state.status);
  }
  if (els.daysView) {
    const days = Number(invite.initialDays);
    els.daysView.textContent = Number.isFinite(days) && days > 0 ? `${days} 天` : "永久有效";
  }
  if (els.expiryView) {
    els.expiryView.textContent = formatDate(invite.expiresAt || "");
  }
}

async function requestJson(url, options = {}) {
  const response = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    if (payload && typeof payload === "object" && payload.error) {
      throw new Error(payload.error);
    }
    throw new Error(`请求失败（${response.status}）`);
  }
  return payload;
}

async function loadInvite() {
  state.inviteCode = parseInviteCode();
  updateView();

  if (!state.inviteCode) {
    setFormEnabled(false);
    setMessage("error", "链接中缺少邀请码。");
    return;
  }

  try {
    const result = await requestJson(`/api/invite/${encodeURIComponent(state.inviteCode)}`);
    state.invite = result && result.invite ? result.invite : null;
    state.status = result && result.status ? result.status : "invalid";
    state.valid = Boolean(result && result.valid);
    updateView();

    if (!state.valid) {
      setFormEnabled(false);
      if (state.status === "used") {
        const suffix = state.invite && state.invite.usedUsername ? `（使用者：${state.invite.usedUsername}）` : "";
        setMessage("warning", `该邀请码已被使用${suffix}。`);
      } else if (state.status === "expired") {
        setMessage("warning", "该邀请码已过期。");
      } else {
        setMessage("error", "该邀请码无效。");
      }
      return;
    }

    setFormEnabled(true);
    setMessage("success", "邀请码校验通过，请填写下方信息完成注册。");
  } catch (err) {
    setFormEnabled(false);
    setMessage("error", `邀请码校验失败：${err.message || "未知错误"}`);
  }
}

async function onSubmit(event) {
  event.preventDefault();

  if (!state.valid || !state.inviteCode) {
    setMessage("warning", "邀请码当前不可用。");
    return;
  }

  const username = (els.usernameInput && els.usernameInput.value || "").trim();
  const password = (els.passwordInput && els.passwordInput.value) || "";
  const confirm = (els.confirmInput && els.confirmInput.value) || "";

  if (username.length < 2) {
    setMessage("warning", "用户名至少需要 2 位。");
    return;
  }
  if (password.length < 6) {
    setMessage("warning", "密码至少需要 6 位。");
    return;
  }
  if (password !== confirm) {
    setMessage("warning", "两次输入的密码不一致。");
    return;
  }

  setFormEnabled(false);
  setMessage("neutral", "正在创建账号...");
  try {
    const result = await requestJson("/api/register", {
      method: "POST",
      body: JSON.stringify({
        inviteCode: state.inviteCode,
        username,
        password,
      }),
    });

    state.valid = false;
    state.status = "used";
    state.invite = {
      ...(state.invite || {}),
      status: "used",
      usedUsername: result && result.user ? result.user.name : username,
      usedAt: result && result.invite ? result.invite.usedAt : "",
    };
    updateView();
    if (els.form) {
      els.form.reset();
    }
    setMessage("success", "注册成功，现在可以登录 Emby。");
  } catch (err) {
    setFormEnabled(true);
    setMessage("error", `注册失败：${err.message || "未知错误"}`);
  }
}

setFormEnabled(false);
if (els.form) {
  els.form.addEventListener("submit", onSubmit);
}
loadInvite();
