document.addEventListener('DOMContentLoaded', () => {
  const jobsTableBody = document.getElementById('jobsTableBody');
  const eventTicker = document.getElementById('eventTicker');
  const jobModal = document.getElementById('jobModal');
  const btnNewJob = document.getElementById('btnNewJob');
  const btnCloseModal = document.getElementById('btnCloseModal');
  const btnCancelModal = document.getElementById('btnCancelModal');
  const jobForm = document.getElementById('jobForm');
  const btnQuickDemo = document.getElementById('btnQuickDemo');
  const btnRefreshJobs = document.getElementById('btnRefreshJobs');

  // Stats Elements
  const statTotalJobs = document.getElementById('statTotalJobs');
  const statRunningJobs = document.getElementById('statRunningJobs');
  const statSuccessRate = document.getElementById('statSuccessRate');

  // Modal Open/Close
  btnNewJob.addEventListener('click', () => jobModal.classList.add('open'));
  btnCloseModal.addEventListener('click', () => jobModal.classList.remove('open'));
  btnCancelModal.addEventListener('click', () => jobModal.classList.remove('open'));

  // Fetch Jobs List
  async function fetchJobs() {
    try {
      const resp = await fetch('/api/v1/jobs?limit=25');
      if (!resp.ok) return;
      const json = await resp.json();
      renderJobs(json.data || []);
      updateStats(json.data || [], json.total || 0);
    } catch (e) {
      console.warn('Failed to fetch jobs:', e);
    }
  }

  function renderJobs(jobs) {
    if (!jobs || jobs.length === 0) {
      jobsTableBody.innerHTML = `
        <tr>
          <td colspan="7" style="text-align: center; color: var(--text-subtle); padding: 24px;">
            No active jobs in queue. Submit a task to begin processing.
          </td>
        </tr>
      `;
      return;
    }

    jobsTableBody.innerHTML = jobs.map(j => {
      const statusClass = `badge-${(j.status || 'pending').toLowerCase()}`;
      const shortID = j.id ? j.id.substring(0, 8) : 'N/A';
      const created = new Date(j.created_at).toLocaleTimeString();

      return `
        <tr>
          <td><span class="code-pill">${shortID}</span></td>
          <td><strong>${escapeHTML(j.task_type)}</strong></td>
          <td>${j.priority}</td>
          <td><span class="badge ${statusClass}">${escapeHTML(j.status)}</span></td>
          <td>${j.attempt_count} / ${j.max_attempts}</td>
          <td>${created}</td>
          <td>
            ${j.status !== 'SUCCEEDED' && j.status !== 'DEAD' && j.status !== 'CANCELLED' ? 
              `<button type="button" class="btn btn-secondary" style="padding: 2px 6px; font-size: 11px;" onclick="cancelJob('${j.id}')">Cancel</button>` : 
              `<span style="color: var(--text-subtle); font-size: 11px;">Done</span>`
            }
          </td>
        </tr>
      `;
    }).join('');
  }

  function updateStats(jobs, total) {
    statTotalJobs.textContent = total;
    const running = jobs.filter(j => j.status === 'RUNNING').length;
    const succeeded = jobs.filter(j => j.status === 'SUCCEEDED').length;
    statRunningJobs.textContent = running;

    if (total > 0) {
      const rate = Math.round((succeeded / Math.max(jobs.length, 1)) * 100);
      statSuccessRate.textContent = `${rate}%`;
    }
  }

  // Submit Job Form Handler
  jobForm.addEventListener('submit', async (e) => {
    e.preventDefault();

    const taskType = document.getElementById('taskType').value;
    const priority = parseInt(document.getElementById('jobPriority').value, 10);
    const maxAttempts = parseInt(document.getElementById('jobMaxAttempts').value, 10);
    const idempotencyKey = document.getElementById('jobIdempotencyKey').value.trim();
    let payload = {};

    try {
      payload = JSON.parse(document.getElementById('jobPayload').value);
    } catch {
      payload = { error: "invalid json" };
    }

    const headers = { 'Content-Type': 'application/json' };
    if (idempotencyKey) {
      headers['Idempotency-Key'] = idempotencyKey;
    }

    try {
      const resp = await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: headers,
        body: JSON.stringify({
          task_type: taskType,
          priority: priority,
          max_attempts: maxAttempts,
          payload: payload
        })
      });

      if (resp.ok) {
        jobModal.classList.remove('open');
        fetchJobs();
      }
    } catch (err) {
      console.error('Job submission failed:', err);
    }
  });

  // Quick Demo Trigger
  btnQuickDemo.addEventListener('click', async () => {
    const demoPayloads = [
      { task_type: "custom-demo", priority: 20, payload: { action: "success", sleep_millis: 100 } },
      { task_type: "notification", priority: 15, payload: { channel: "webhook", target: "https://api.internal/hooks", message: "Job processed" } },
      { task_type: "database-backup", priority: 5, payload: { database: "core_db", target_s3: "s3://backups/core" } }
    ];

    const pick = demoPayloads[Math.floor(Math.random() * demoPayloads.length)];
    await fetch('/api/v1/jobs', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(pick)
    });

    fetchJobs();
  });

  btnRefreshJobs.addEventListener('click', fetchJobs);

  // Window helper for cancelling
  window.cancelJob = async (id) => {
    await fetch(`/api/v1/jobs/${id}/cancel`, { method: 'POST' });
    fetchJobs();
  };

  // Connect Server-Sent Events Stream
  function connectSSE() {
    const eventSource = new EventSource('/api/v1/events/stream');

    eventSource.onopen = () => {
      document.getElementById('streamStatusText').textContent = 'Connected';
      document.getElementById('streamStatusDot').style.background = 'var(--status-success)';
    };

    eventSource.onmessage = (event) => {
      appendEvent(event.data);
      fetchJobs();
    };

    eventSource.onerror = () => {
      document.getElementById('streamStatusText').textContent = 'Reconnecting...';
      document.getElementById('streamStatusDot').style.background = 'var(--status-warning)';
    };
  }

  function appendEvent(data) {
    const timeStr = new Date().toLocaleTimeString();
    const row = document.createElement('div');
    row.className = 'event-row';
    row.innerHTML = `
      <span class="event-time">${timeStr}</span>
      <span class="event-tag">EVENT</span>
      <span>${escapeHTML(data)}</span>
    `;

    eventTicker.insertBefore(row, eventTicker.firstChild);
    if (eventTicker.children.length > 50) {
      eventTicker.removeChild(eventTicker.lastChild);
    }
  }

  function escapeHTML(str) {
    if (!str) return '';
    return String(str).replace(/[&<>'"]/g, 
      tag => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[tag] || tag)
    );
  }

  // Initial load
  fetchJobs();
  connectSSE();
  setInterval(fetchJobs, 3000);
});
