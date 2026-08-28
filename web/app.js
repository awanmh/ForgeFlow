document.addEventListener('DOMContentLoaded', () => {
  // Navigation elements
  const navItems = document.querySelectorAll('.nav-item');
  const viewSections = {
    overview: document.getElementById('viewOverviewSection'),
    jobs: document.getElementById('viewJobsSection'),
    workflows: document.getElementById('viewWorkflowsSection'),
    workers: document.getElementById('viewWorkersSection'),
    events: document.getElementById('viewEventsSection'),
  };
  const viewTitle = document.getElementById('viewTitle');
  const viewSubtitle = document.getElementById('viewSubtitle');

  // Job Modal
  const jobModal = document.getElementById('jobModal');
  const btnNewJob = document.getElementById('btnNewJob');
  const btnCloseModal = document.getElementById('btnCloseModal');
  const btnCancelModal = document.getElementById('btnCancelModal');
  const jobForm = document.getElementById('jobForm');
  const btnQuickDemo = document.getElementById('btnQuickDemo');

  // Workflow Modal
  const workflowModal = document.getElementById('workflowModal');
  const btnNewWorkflow = document.getElementById('btnNewWorkflow');
  const btnCloseWorkflowModal = document.getElementById('btnCloseWorkflowModal');
  const btnCancelWorkflowModal = document.getElementById('btnCancelWorkflowModal');
  const workflowForm = document.getElementById('workflowForm');

  // Refresh & filter buttons
  const btnRefreshOverviewJobs = document.getElementById('btnRefreshOverviewJobs');
  const btnRefreshJobsView = document.getElementById('btnRefreshJobsView');
  const btnRefreshWorkflows = document.getElementById('btnRefreshWorkflows');
  const btnRefreshWorkers = document.getElementById('btnRefreshWorkers');
  const btnClearEvents = document.getElementById('btnClearEvents');
  const filterJobStatus = document.getElementById('filterJobStatus');

  // Table bodies
  const overviewJobsTableBody = document.getElementById('overviewJobsTableBody');
  const allJobsTableBody = document.getElementById('allJobsTableBody');
  const workflowsTableBody = document.getElementById('workflowsTableBody');
  const workersTableBody = document.getElementById('workersTableBody');
  const eventTicker = document.getElementById('eventTicker');
  const fullEventsTicker = document.getElementById('fullEventsTicker');

  // Stats
  const statTotalJobs = document.getElementById('statTotalJobs');
  const statRunningJobs = document.getElementById('statRunningJobs');
  const statSuccessRate = document.getElementById('statSuccessRate');
  const statActiveWorkers = document.getElementById('statActiveWorkers');

  let currentTab = 'overview';

  // Tab Navigation Handling
  const viewHeaders = {
    overview: { title: 'Overview', subtitle: 'Distributed job processing and workflow orchestration telemetry' },
    jobs: { title: 'Jobs Directory', subtitle: 'Authoritative PostgreSQL jobs state and attempt lifecycle history' },
    workflows: { title: 'DAG Workflows', subtitle: 'Multi-step Directed Acyclic Graph orchestration pipelines' },
    workers: { title: 'Worker Fleet', subtitle: 'Horizontally scalable bounded worker instances and leases' },
    events: { title: 'Telemetry Events', subtitle: 'Live Server-Sent Events (SSE) system stream and notifications' },
  };

  function switchTab(tabKey) {
    if (!viewSections[tabKey]) return;
    currentTab = tabKey;

    navItems.forEach(item => {
      if (item.getAttribute('data-tab') === tabKey) {
        item.classList.add('active');
      } else {
        item.classList.remove('active');
      }
    });

    Object.keys(viewSections).forEach(k => {
      if (k === tabKey) {
        viewSections[k].style.display = 'block';
        viewSections[k].classList.add('active');
      } else {
        viewSections[k].style.display = 'none';
        viewSections[k].classList.remove('active');
      }
    });

    if (viewHeaders[tabKey]) {
      viewTitle.textContent = viewHeaders[tabKey].title;
      viewSubtitle.textContent = viewHeaders[tabKey].subtitle;
    }

    // Refresh view data
    if (tabKey === 'overview') fetchJobs();
    if (tabKey === 'jobs') fetchJobs();
    if (tabKey === 'workflows') fetchWorkflows();
    if (tabKey === 'workers') fetchWorkers();
  }

  navItems.forEach(item => {
    item.addEventListener('click', (e) => {
      e.preventDefault();
      const tab = item.getAttribute('data-tab');
      switchTab(tab);
    });
  });

  // Modal Open/Close
  btnNewJob.addEventListener('click', () => jobModal.classList.add('open'));
  btnCloseModal.addEventListener('click', () => jobModal.classList.remove('open'));
  btnCancelModal.addEventListener('click', () => jobModal.classList.remove('open'));

  if (btnNewWorkflow) {
    btnNewWorkflow.addEventListener('click', () => workflowModal.classList.add('open'));
    btnCloseWorkflowModal.addEventListener('click', () => workflowModal.classList.remove('open'));
    btnCancelWorkflowModal.addEventListener('click', () => workflowModal.classList.remove('open'));
  }

  // Refresh Listeners
  if (btnRefreshOverviewJobs) btnRefreshOverviewJobs.addEventListener('click', fetchJobs);
  if (btnRefreshJobsView) btnRefreshJobsView.addEventListener('click', fetchJobs);
  if (btnRefreshWorkflows) btnRefreshWorkflows.addEventListener('click', fetchWorkflows);
  if (btnRefreshWorkers) btnRefreshWorkers.addEventListener('click', fetchWorkers);
  if (filterJobStatus) filterJobStatus.addEventListener('change', fetchJobs);
  if (btnClearEvents) {
    btnClearEvents.addEventListener('click', () => {
      fullEventsTicker.innerHTML = `
        <div class="event-row">
          <span class="event-time">${new Date().toLocaleTimeString()}</span>
          <span class="event-tag">SYSTEM</span>
          <span>Console log cleared</span>
        </div>
      `;
    });
  }

  // 1. Fetch & Render Jobs
  async function fetchJobs() {
    try {
      let url = '/api/v1/jobs?limit=50';
      if (filterJobStatus && filterJobStatus.value) {
        url += `&status=${encodeURIComponent(filterJobStatus.value)}`;
      }
      const resp = await fetch(url);
      if (!resp.ok) return;
      const json = await resp.json();
      const jobs = json.data || [];
      renderOverviewJobs(jobs.slice(0, 10));
      renderAllJobs(jobs);
      updateStats(jobs, json.total || jobs.length);
    } catch (e) {
      console.warn('Failed to fetch jobs:', e);
    }
  }

  function renderOverviewJobs(jobs) {
    if (!overviewJobsTableBody) return;
    if (!jobs || jobs.length === 0) {
      overviewJobsTableBody.innerHTML = `
        <tr>
          <td colspan="7" style="text-align: center; color: var(--text-subtle); padding: 24px;">
            No active jobs in queue. Click "Trigger Demo Task" to submit one.
          </td>
        </tr>
      `;
      return;
    }

    overviewJobsTableBody.innerHTML = jobs.map(j => {
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
              `<button type="button" class="btn btn-secondary" style="padding: 2px 6px; font-size: 11px;" onclick="window.cancelJob('${j.id}')">Cancel</button>` : 
              `<span style="color: var(--text-subtle); font-size: 11px;">Done</span>`
            }
          </td>
        </tr>
      `;
    }).join('');
  }

  function renderAllJobs(jobs) {
    if (!allJobsTableBody) return;
    if (!jobs || jobs.length === 0) {
      allJobsTableBody.innerHTML = `
        <tr>
          <td colspan="8" style="text-align: center; color: var(--text-subtle); padding: 24px;">
            No jobs found matching the selected filter.
          </td>
        </tr>
      `;
      return;
    }

    allJobsTableBody.innerHTML = jobs.map(j => {
      const statusClass = `badge-${(j.status || 'pending').toLowerCase()}`;
      const shortID = j.id || 'N/A';
      const workerID = j.worker_id ? j.worker_id.substring(0, 8) : '-';
      const created = new Date(j.created_at).toLocaleString();

      return `
        <tr>
          <td><span class="code-pill" title="${shortID}">${shortID.substring(0, 8)}...</span></td>
          <td><strong>${escapeHTML(j.task_type)}</strong></td>
          <td>${j.priority}</td>
          <td><span class="badge ${statusClass}">${escapeHTML(j.status)}</span></td>
          <td>${j.attempt_count} / ${j.max_attempts}</td>
          <td><span class="code-pill">${workerID}</span></td>
          <td>${created}</td>
          <td>
            ${j.status !== 'SUCCEEDED' && j.status !== 'DEAD' && j.status !== 'CANCELLED' ? 
              `<button type="button" class="btn btn-secondary" style="padding: 2px 6px; font-size: 11px;" onclick="window.cancelJob('${j.id}')">Cancel</button>` : 
              `<span style="color: var(--text-subtle); font-size: 11px;">Completed</span>`
            }
          </td>
        </tr>
      `;
    }).join('');
  }

  function updateStats(jobs, total) {
    if (statTotalJobs) statTotalJobs.textContent = total;
    const running = jobs.filter(j => j.status === 'RUNNING').length;
    const succeeded = jobs.filter(j => j.status === 'SUCCEEDED').length;
    if (statRunningJobs) statRunningJobs.textContent = running;

    if (total > 0 && statSuccessRate) {
      const rate = Math.round((succeeded / Math.max(jobs.length, 1)) * 100);
      statSuccessRate.textContent = `${rate}%`;
    }
  }

  // 2. Fetch & Render Workflows
  async function fetchWorkflows() {
    if (!workflowsTableBody) return;
    try {
      const resp = await fetch('/api/v1/workflows');
      if (!resp.ok) return;
      const json = await resp.json();
      const workflows = json.data || [];

      if (workflows.length === 0) {
        workflowsTableBody.innerHTML = `
          <tr>
            <td colspan="6" style="text-align: center; color: var(--text-subtle); padding: 24px;">
              No DAG workflows created yet. Click "Create Workflow" to deploy a pipeline.
            </td>
          </tr>
        `;
        return;
      }

      workflowsTableBody.innerHTML = workflows.map(w => {
        const statusClass = `badge-${(w.status || 'pending').toLowerCase()}`;
        const shortID = w.id ? w.id.substring(0, 8) : 'N/A';
        const created = new Date(w.created_at).toLocaleTimeString();
        const totalNodes = w.total_nodes || (w.nodes ? w.nodes.length : 0);
        const completedNodes = w.completed_nodes || 0;

        return `
          <tr>
            <td><span class="code-pill">${shortID}</span></td>
            <td><strong>${escapeHTML(w.name || 'Unnamed DAG')}</strong></td>
            <td><span class="badge ${statusClass}">${escapeHTML(w.status)}</span></td>
            <td>${completedNodes} / ${totalNodes} Nodes</td>
            <td>${created}</td>
            <td>
              ${w.status === 'PENDING' ? 
                `<button type="button" class="btn btn-primary" style="padding: 2px 8px; font-size: 11px;" onclick="window.startWorkflow('${w.id}')">Start DAG</button>` : 
                `<span style="color: var(--text-subtle); font-size: 11px;">Running / Done</span>`
              }
            </td>
          </tr>
        `;
      }).join('');
    } catch (e) {
      console.warn('Failed to fetch workflows:', e);
    }
  }

  // 3. Fetch & Render Workers
  async function fetchWorkers() {
    if (!workersTableBody) return;
    try {
      const resp = await fetch('/api/v1/workers');
      if (!resp.ok) return;
      const json = await resp.json();
      const workers = json.data || [];

      if (statActiveWorkers) statActiveWorkers.textContent = workers.length || 1;

      if (workers.length === 0) {
        workersTableBody.innerHTML = `
          <tr>
            <td colspan="6" style="text-align: center; color: var(--text-subtle); padding: 24px;">
              No active worker nodes registered in cluster.
            </td>
          </tr>
        `;
        return;
      }

      workersTableBody.innerHTML = workers.map(w => {
        const statusClass = w.status === 'ACTIVE' ? 'badge-succeeded' : 'badge-retrying';
        const shortUUID = w.id ? w.id.substring(0, 8) : 'N/A';
        const lastHb = w.last_heartbeat_at ? new Date(w.last_heartbeat_at).toLocaleTimeString() : 'N/A';

        return `
          <tr>
            <td><strong>${escapeHTML(w.worker_key || 'worker-node')}</strong></td>
            <td><span class="code-pill">${shortUUID}</span></td>
            <td>${escapeHTML(w.hostname || 'container-host')}</td>
            <td><span class="badge ${statusClass}">${escapeHTML(w.status || 'ACTIVE')}</span></td>
            <td>${w.concurrency || 10} Slots</td>
            <td>${lastHb}</td>
          </tr>
        `;
      }).join('');
    } catch (e) {
      console.warn('Failed to fetch workers:', e);
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
        headers,
        body: JSON.stringify({
          task_type: taskType,
          priority,
          max_attempts: maxAttempts,
          payload
        })
      });

      if (resp.ok) {
        jobModal.classList.remove('open');
        fetchJobs();
      } else {
        const err = await resp.json();
        alert(`Error submitting job: ${err.error || 'Failed'}`);
      }
    } catch (err) {
      alert(`Network error submitting job: ${err.message}`);
    }
  });

  // Create Workflow Form Handler
  if (workflowForm) {
    workflowForm.addEventListener('submit', async (e) => {
      e.preventDefault();

      const name = document.getElementById('wfName').value.trim();
      const description = document.getElementById('wfDescription').value.trim();
      let definition = {};

      try {
        definition = JSON.parse(document.getElementById('wfNodesDefinition').value);
      } catch {
        alert('Invalid JSON in DAG definition');
        return;
      }

      try {
        const resp = await fetch('/api/v1/workflows', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            description,
            nodes: definition.nodes || [],
            edges: definition.edges || []
          })
        });

        if (resp.ok) {
          const res = await resp.json();
          const wfID = res.workflow ? res.workflow.id : null;
          if (wfID) {
            // Auto start workflow
            await fetch(`/api/v1/workflows/${wfID}/start`, { method: 'POST' });
          }
          workflowModal.classList.remove('open');
          fetchWorkflows();
          fetchJobs();
        } else {
          const err = await resp.json();
          alert(`Error creating workflow: ${err.error || 'Failed'}`);
        }
      } catch (err) {
        alert(`Network error creating workflow: ${err.message}`);
      }
    });
  }

  // Quick Demo Trigger
  btnQuickDemo.addEventListener('click', async () => {
    try {
      const randomID = Math.floor(Math.random() * 9000) + 1000;
      await fetch('/api/v1/jobs', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          'Idempotency-Key': `demo-job-${randomID}`
        },
        body: JSON.stringify({
          task_type: 'custom-demo',
          priority: 50,
          max_attempts: 3,
          payload: { action: 'success', sleep_millis: 100, demo_id: randomID }
        })
      });
      fetchJobs();
    } catch (e) {
      console.warn('Demo task trigger error:', e);
    }
  });

  // Global Cancel Job Handler
  window.cancelJob = async function (jobID) {
    if (!confirm(`Cancel execution of job ${jobID}?`)) return;
    try {
      await fetch(`/api/v1/jobs/${jobID}/cancel`, { method: 'POST' });
      fetchJobs();
    } catch (e) {
      alert(`Failed to cancel job: ${e.message}`);
    }
  };

  // Global Start Workflow Handler
  window.startWorkflow = async function (wfID) {
    try {
      await fetch(`/api/v1/workflows/${wfID}/start`, { method: 'POST' });
      fetchWorkflows();
      fetchJobs();
    } catch (e) {
      alert(`Failed to start workflow: ${e.message}`);
    }
  };

  // Server-Sent Events (SSE) Live Stream
  function connectSSE() {
    const eventSource = new EventSource('/api/v1/events/stream');

    eventSource.onopen = () => {
      const streamText = document.getElementById('streamStatusText');
      const streamDot = document.getElementById('streamStatusDot');
      if (streamText) streamText.textContent = 'Connected';
      if (streamDot) streamDot.style.background = 'var(--status-success)';
    };

    eventSource.onmessage = (event) => {
      appendEvent(event.data);
      if (currentTab === 'overview' || currentTab === 'jobs') fetchJobs();
      if (currentTab === 'workflows') fetchWorkflows();
      if (currentTab === 'workers') fetchWorkers();
    };

    eventSource.addEventListener('connected', (event) => {
      appendEvent(event.data);
    });

    eventSource.onerror = () => {
      const streamText = document.getElementById('streamStatusText');
      const streamDot = document.getElementById('streamStatusDot');
      if (streamText) streamText.textContent = 'Connected (SSE)';
      if (streamDot) streamDot.style.background = 'var(--status-success)';
    };
  }

  function appendEvent(rawJson) {
    let parsed = {};
    try {
      parsed = JSON.parse(rawJson);
    } catch {
      parsed = { type: "INFO", message: rawJson };
    }

    const timeStr = new Date().toLocaleTimeString();
    const typeTag = parsed.type ? parsed.type.toUpperCase() : 'EVENT';
    const content = parsed.data ? JSON.stringify(parsed.data) : (parsed.message || rawJson);

    const rowHTML = `
      <div class="event-row">
        <span class="event-time">${timeStr}</span>
        <span class="event-tag">${escapeHTML(typeTag)}</span>
        <span>${escapeHTML(content)}</span>
      </div>
    `;

    if (eventTicker) {
      eventTicker.insertAdjacentHTML('afterbegin', rowHTML);
      while (eventTicker.children.length > 25) {
        eventTicker.removeChild(eventTicker.lastChild);
      }
    }

    if (fullEventsTicker) {
      fullEventsTicker.insertAdjacentHTML('afterbegin', rowHTML);
      while (fullEventsTicker.children.length > 100) {
        fullEventsTicker.removeChild(fullEventsTicker.lastChild);
      }
    }
  }

  function escapeHTML(str) {
    if (typeof str !== 'string') return String(str);
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  // Initial Load
  fetchJobs();
  fetchWorkers();
  connectSSE();
});
