const state = {
    fieldMapping: {},
};

function getEl(id) {
    return document.getElementById(id);
}

async function api(url, method = "GET", body = null) {
    const opts = { method, headers: {} };
    if (body && !(body instanceof FormData)) {
        opts.headers["Content-Type"] = "application/json";
        opts.body = JSON.stringify(body);
    } else if (body) {
        opts.body = body;
    }

    const res = await fetch(url, opts);
    let data = {};
    try {
        data = await res.json();
    } catch (e) {
        data = {};
    }

    if (!res.ok) {
        throw new Error(data.error || `Request failed (${res.status})`);
    }

    return data;
}

function showToast(msg, type = "success") {
    const toast = getEl("toast");
    toast.textContent = msg;
    toast.className = `toast toast-${type} toast-show`;
    setTimeout(() => {
        toast.className = "toast";
    }, 3500);
}

function setButtonLoading(id, isLoading) {
    const btn = getEl(id);
    if (!btn) {
        return;
    }
    btn.classList.toggle("btn-loading", isLoading);
    btn.disabled = isLoading;
}

function createMappingRow(placeholder = "", fieldName = "") {
    const row = document.createElement("div");
    row.className = "mapping-row";
    row.innerHTML = `
        <input type="text" class="mapping-input mapping-placeholder" placeholder="{{Google Docs Placeholder}}" value="${escapeHtml(placeholder)}">
        <input type="text" class="mapping-input mapping-field" placeholder="Airtable Field Name" value="${escapeHtml(fieldName)}">
        <button type="button" class="mapping-remove" aria-label="Remove mapping">×</button>
    `;

    row.querySelector(".mapping-remove").addEventListener("click", () => {
        row.remove();
    });
    return row;
}

function renderFieldMappings(mapping) {
    const list = getEl("mappingList");
    list.innerHTML = "";
    Object.entries(mapping).forEach(([placeholder, fieldName]) => {
        list.appendChild(createMappingRow(placeholder, fieldName));
    });
    if (!Object.keys(mapping).length) {
        list.appendChild(createMappingRow());
    }
}

function collectFieldMappings() {
    const mapping = {};
    document.querySelectorAll(".mapping-row").forEach((row) => {
        const placeholder = row.querySelector(".mapping-placeholder").value.trim();
        const fieldName = row.querySelector(".mapping-field").value.trim();
        if (placeholder && fieldName) {
            mapping[placeholder] = fieldName;
        }
    });
    return mapping;
}

async function loadConfig() {
    const data = await api("/api/config");
    state.fieldMapping = data.field_mapping || {};

    const fields = [
        "base_id",
        "table_name",
        "view_name",
        "attachment_field_name",
        "template_doc_id",
        "target_folder_id",
    ];
    fields.forEach((field) => {
        const el = getEl(field);
        if (el) {
            el.value = data[field] || "";
        }
    });

    const tokenInput = getEl("airtable_api_token");
    const tokenHint = getEl("tokenHint");
    tokenInput.value = "";
    if (data.airtable_api_token_masked) {
        tokenInput.placeholder = data.airtable_api_token_masked;
        tokenHint.textContent = `Current token: ${data.airtable_api_token_masked}. Leave blank to keep it.`;
    } else {
        tokenInput.placeholder = "pat...";
        tokenHint.textContent = "Leave blank to keep the saved token on the server.";
    }

    if (data.interval_minutes) {
        getEl("interval_minutes").value = data.interval_minutes;
    }

    renderFieldMappings(state.fieldMapping);
}

async function saveConfig() {
    const body = {
        base_id: getEl("base_id").value,
        table_name: getEl("table_name").value,
        view_name: getEl("view_name").value,
        attachment_field_name: getEl("attachment_field_name").value,
        template_doc_id: getEl("template_doc_id").value,
        target_folder_id: getEl("target_folder_id").value,
        interval_minutes: getEl("interval_minutes").value,
        field_mapping: collectFieldMappings(),
    };

    const token = getEl("airtable_api_token").value.trim();
    if (token) {
        body.airtable_api_token = token;
    }

    setButtonLoading("saveBtn", true);
    try {
        await api("/api/config", "POST", body);
        showToast("Configuration saved successfully");
        getEl("airtable_api_token").value = "";
        await loadConfig();
    } catch (e) {
        showToast(e.message, "error");
    } finally {
        setButtonLoading("saveBtn", false);
    }
}

function setupUploadZone() {
    const zone = getEl("uploadZone");
    const input = getEl("credentialsFile");

    zone.addEventListener("click", () => input.click());
    zone.addEventListener("dragover", (e) => {
        e.preventDefault();
        zone.classList.add("drag-over");
    });
    zone.addEventListener("dragleave", () => zone.classList.remove("drag-over"));
    zone.addEventListener("drop", (e) => {
        e.preventDefault();
        zone.classList.remove("drag-over");
        if (e.dataTransfer.files.length) {
            uploadFile(e.dataTransfer.files[0]);
        }
    });
    input.addEventListener("change", () => {
        if (input.files.length) {
            uploadFile(input.files[0]);
        }
    });
}

async function uploadFile(file) {
    const fd = new FormData();
    fd.append("file", file);
    try {
        const data = await api("/api/upload-credentials", "POST", fd);
        showToast("credentials.json uploaded and OAuth reset");
        updateCredentialsInfo(data.info);
    } catch (e) {
        showToast(e.message, "error");
    }
}

function updateCredentialsInfo(info) {
    const el = getEl("credentialsInfo");
    if (info && info.exists) {
        el.innerHTML = `
            <div class="cred-badge cred-ok">
                <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><polyline points="20 6 9 17 4 12"/></svg>
                Active
            </div>
            <span class="cred-detail">Uploaded ${escapeHtml(info.uploaded_at || "just now")}</span>`;
        return;
    }

    el.innerHTML = `
        <div class="cred-badge cred-missing">
            <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5"><circle cx="12" cy="12" r="10"/><line x1="15" y1="9" x2="9" y2="15"/><line x1="9" y1="9" x2="15" y2="15"/></svg>
            Not uploaded
        </div>`;
}

function updateUI(status) {
    const badge = getEl("statusBadge");
    const statusText = getEl("statusText");
    const startBtn = getEl("startBtn");
    const stopBtn = getEl("stopBtn");
    const runNowBtn = getEl("runNowBtn");

    if (status.scheduler_running) {
        statusText.textContent = status.processing_active ? "Processing" : "Running";
        badge.className = "status-badge status-running";
        startBtn.disabled = true;
        stopBtn.disabled = false;
    } else {
        statusText.textContent = status.processing_active ? "Processing" : "Stopped";
        badge.className = status.processing_active ? "status-badge status-running" : "status-badge status-stopped";
        startBtn.disabled = status.processing_active;
        stopBtn.disabled = true;
    }

    runNowBtn.disabled = Boolean(status.processing_active);
}

async function startScheduler() {
    try {
        const res = await api("/api/start", "POST");
        showToast(res.message || "Scheduler started");
        await refreshStatus();
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function stopScheduler() {
    try {
        const res = await api("/api/stop", "POST");
        showToast(res.message || "Scheduler stopped");
        await refreshStatus();
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function runNow() {
    try {
        const res = await api("/api/run-now", "POST");
        showToast(res.message || "Manual run started");
        await refreshStatus();
    } catch (e) {
        showToast(e.message, "error");
    }
}

async function refreshStatus() {
    try {
        const data = await api("/api/status");
        updateUI(data);
        updateCredentialsInfo(data.credentials);
        getEl("statLastRun").textContent = data.last_run || "—";
        if (data.last_stats) {
            animateNumber("statProcessed", data.last_stats.processed || 0);
            animateNumber("statSkipped", data.last_stats.skipped || 0);
            animateNumber("statErrors", data.last_stats.errors || 0);
        }
    } catch (e) {
        showToast(`Status refresh failed: ${e.message}`, "error");
    }
}

function animateNumber(id, target) {
    const el = getEl(id);
    const current = parseInt(el.textContent, 10) || 0;
    if (current === target) {
        return;
    }
    el.textContent = target;
    el.classList.add("stat-pop");
    setTimeout(() => el.classList.remove("stat-pop"), 400);
}

async function refreshLogs() {
    try {
        const data = await api("/api/logs?lines=100");
        const container = getEl("logsContent");
        if (data.logs && data.logs.length) {
            container.innerHTML = data.logs.map((line) => {
                if (line.includes("[ERROR]")) {
                    return `<span class="log-error">${escapeHtml(line)}</span>`;
                }
                if (line.includes("[WARNING]")) {
                    return `<span class="log-warn">${escapeHtml(line)}</span>`;
                }
                if (line.includes("complete") || line.includes("successful")) {
                    return `<span class="log-success">${escapeHtml(line)}</span>`;
                }
                return escapeHtml(line);
            }).join("\n");
        } else {
            container.textContent = "Awaiting logs...";
        }
        const logsDiv = getEl("logsContainer");
        logsDiv.scrollTop = logsDiv.scrollHeight;
    } catch (e) {
        getEl("logsContent").textContent = `Unable to load logs: ${e.message}`;
    }
}

function escapeHtml(value) {
    return String(value)
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#39;");
}

function initTilt() {
    const prefersReducedMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    if (prefersReducedMotion || window.matchMedia("(pointer: coarse)").matches) {
        return;
    }

    document.querySelectorAll("[data-tilt]").forEach((card) => {
        card.addEventListener("mousemove", (e) => {
            const rect = card.getBoundingClientRect();
            const x = (e.clientX - rect.left) / rect.width - 0.5;
            const y = (e.clientY - rect.top) / rect.height - 0.5;
            card.style.transform = `perspective(800px) rotateY(${x * 4}deg) rotateX(${-y * 4}deg) translateY(-4px)`;
            const glow = card.querySelector(".card-glow");
            if (glow) {
                glow.style.background = `radial-gradient(circle at ${(x + 0.5) * 100}% ${(y + 0.5) * 100}%, rgba(99, 102, 241, 0.08), transparent 60%)`;
            }
        });
        card.addEventListener("mouseleave", () => {
            card.style.transform = "";
            const glow = card.querySelector(".card-glow");
            if (glow) {
                glow.style.background = "";
            }
        });
    });
}

function setupFieldMappingEditor() {
    getEl("addMappingBtn").addEventListener("click", () => {
        getEl("mappingList").appendChild(createMappingRow());
    });
}

window.startScheduler = startScheduler;
window.stopScheduler = stopScheduler;
window.runNow = runNow;
window.saveConfig = saveConfig;
window.refreshLogs = refreshLogs;

window.addEventListener("DOMContentLoaded", async () => {
    setupUploadZone();
    setupFieldMappingEditor();
    initTilt();

    try {
        await loadConfig();
        await refreshStatus();
        await refreshLogs();
    } catch (e) {
        showToast(e.message, "error");
    }

    setInterval(refreshStatus, 5000);
    setInterval(refreshLogs, 5000);
});
