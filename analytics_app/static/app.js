(function () {
  const managementRaw = document.getElementById("management-chart-data");
  if (managementRaw && window.Chart) {
    const data = JSON.parse(managementRaw.textContent);
    const makeBar = (id, label, payload, color) => {
      const node = document.getElementById(id);
      if (!node) return;
      new Chart(node, {
        type: "bar",
        data: {
          labels: payload.labels || [],
          datasets: [{
            label,
            data: payload.values || [],
            backgroundColor: color || "#2563eb"
          }]
        },
        options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } } }
      });
    };
    makeBar("waterfallChart", "₽", data.waterfall || {}, "#2563eb");
    makeBar("expenseGroupsChart", "₽", data.expense_groups || {}, "#dc6803");
    makeBar("salaryByMasterChart", "₽", data.salary_by_master || {}, "#039855");
    makeBar("timeSeriesChart", "₽", data.time_series || {}, "#7c3aed");
  }

  const raw = document.getElementById("chart-data");
  if (raw && window.Chart) {
    const data = JSON.parse(raw.textContent);
    const makeChart = (id, title, payload) => {
      const node = document.getElementById(id);
      if (!node) return;
      new Chart(node, {
        type: "bar",
        data: {
          labels: payload.labels,
          datasets: [{ label: title, data: payload.values, backgroundColor: "#4f7cff" }]
        },
        options: { responsive: true, plugins: { legend: { display: false } } }
      });
    };
    makeChart("incomeChart", "Доходы", data.income);
    makeChart("expenseChart", "Расходы", data.expense);
  }

  const search = document.getElementById("ledgerSearch");
  const table = document.getElementById("ledgerTable");
  if (search && table) {
    search.addEventListener("input", function () {
      const needle = search.value.toLowerCase();
      for (const row of table.querySelectorAll("tbody tr")) {
        row.style.display = row.textContent.toLowerCase().includes(needle) ? "" : "none";
      }
    });
  }
})();
