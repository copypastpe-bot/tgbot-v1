(function () {
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
