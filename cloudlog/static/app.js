(function () {
  function wireTaskFilter(root) {
    const parent = root.querySelector("[data-task-parent]");
    const child = root.querySelector("[data-task-child]");
    if (!parent || !child) return;

    function refresh() {
      const pid = String(parent.value || "");
      const options = Array.from(child.options);
      options.forEach((opt) => {
        const project = opt.getAttribute("data-project");
        if (!project) {
          opt.hidden = false;
          return;
        }
        opt.hidden = pid && project !== pid;
        if (opt.hidden && opt.selected) {
          child.value = "";
        }
      });
    }

    parent.addEventListener("change", refresh);
    refresh();
  }

  document.querySelectorAll("form").forEach(wireTaskFilter);
})();
