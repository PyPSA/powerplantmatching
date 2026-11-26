// SPDX-FileCopyrightText: Contributors to powerplantmatching <https://github.com/pypsa/powerplantmatching>
//
// SPDX-License-Identifier: MIT

// Sortable tables on all pages, arrows in delta columns and heatmap shading
// of the delta columns in tabbed tables (data-change reports). Numbers group
// thousands with a narrow no-break space (U+202F), e.g. "1 450.431", which
// Tablesort cannot parse, so numeric cells carry the plain number in data-sort.

// Runs on `document$` (MkDocs Material, instant navigation) or, in the
// standalone report.html, on DOMContentLoaded.

// Colour classes of a change, from the largest loss (orange) to the largest
// gain (blue), no change in the middle. Keep in sync with CLASS_COLORS in
// powerplantmatching/changes.py, which explains the choice.
const CLASS_COLORS = [
  "#8e5318", "#bd7634", "#e0a26f", "#f6cfb0",
  "#f0f0f0",
  "#badbfe", "#7fb6ee", "#4b8dcf", "#2e669c",
];
// Size classes per sign, one per decade of the change
const CLASSES = (CLASS_COLORS.length - 1) / 2;
const POSITIVE = CLASS_COLORS.at(-1);
const NEGATIVE = CLASS_COLORS[0];

// Number of a cell, e.g. 1450.5 of "+1 450.5 (+3%)", or null for other
// cells. The whole cell must be a number, optionally with a change in % in
// brackets, so a name like "2nd unit" stays text. data-sort holds the number
// once markNumbers ran.
function cellValue(cell) {
  const text = (cell.dataset.sort ?? cell.textContent).replaceAll("\u202F", "");
  const match = text.trim().match(/^([+-]?\d+(\.\d+)?)( \([+-]?\d+(\.\d+)?%\))?$/);
  return match ? Number(match[1]) : null;
}

function headersOf(table) {
  return Array.from(table.tHead.rows[0].cells, (th) => th.textContent);
}

// Store the plain number of each numeric cell in data-sort, which Tablesort
// reads before the text. In delta columns also prefix the cell with an arrow:
// up for a gain, down for a loss, a neutral dot for no change. The arrow
// repeats the sign, which stays in the text.
function markNumbers(table) {
  const deltas = headersOf(table).map((header) => /delta/i.test(header));
  for (const row of table.tBodies[0].rows) {
    Array.from(row.cells).forEach((cell, index) => {
      const value = cellValue(cell);
      if (value === null) return;
      cell.dataset.sort = String(value);
      if (!deltas[index]) return;
      const arrow = document.createElement("span");
      arrow.setAttribute("aria-hidden", "true");
      arrow.style.marginRight = "0.25em";
      if (value === 0) {
        arrow.textContent = "\u25CF"; // dot
        arrow.style.color = "var(--md-default-fg-color--light)";
      } else {
        arrow.textContent = value > 0 ? "\u25B2" : "\u25BC"; // up, down
        arrow.style.color = value > 0 ? POSITIVE : NEGATIVE;
      }
      cell.prepend(arrow);
    });
  }
}

// Lower bounds of the size classes 2 to CLASSES of changes up to peak > 0:
// whole decades, the largest class from one decade below the decade of the
// peak, e.g. 0.1, 1 and 10 for a peak of 103. As _class_bounds in changes.py.
function classBounds(peak) {
  const decade = Math.floor(Math.log10(peak));
  return Array.from({ length: CLASSES - 1 }, (_, i) => 10 ** (decade - CLASSES + 1 + i));
}

// Shade each delta column by the size class of the change (classBounds of the
// largest |v| of the column), so small changes stay visible next to large
// ones. No change stays unshaded, its dot marks it. Styles sit on the <td>,
// so they move with the rows when sorting.
function shadeDeltas(table) {
  const rows = Array.from(table.tBodies[0].rows);
  headersOf(table).forEach((header, index) => {
    if (!/delta/i.test(header)) return;
    const max = Math.max(
      0, ...rows.map((row) => Math.abs(cellValue(row.cells[index]) ?? 0)),
    );
    if (max === 0) return;
    const bounds = classBounds(max);
    for (const row of rows) {
      const cell = row.cells[index];
      const value = cellValue(cell);
      if (!value) continue; // null or no change
      const size = 1 + bounds.filter((bound) => Math.abs(value) >= bound).length;
      cell.style.backgroundColor = CLASS_COLORS[CLASSES + Math.sign(value) * size];
      // Text contrast: white on the dark outer class, black on the others. The
      // arrow (first child, see markNumbers) takes the text colour, as the
      // background now carries the colour of the sign.
      cell.style.color = size === CLASSES ? "#fff" : "#000";
      cell.firstElementChild.style.color = "inherit";
    }
  });
}

function init() {
  // The muted colour of arrows for "no change" outside MkDocs Material.
  const root = document.documentElement;
  if (!getComputedStyle(root).getPropertyValue("--md-default-fg-color--light")) {
    root.style.setProperty("--md-default-fg-color--light", "#888");
  }
  for (const table of document.querySelectorAll("article table:not([class])")) {
    markNumbers(table);
    new Tablesort(table);
  }
  for (const table of document.querySelectorAll("article .tabbed-block table")) {
    shadeDeltas(table);
  }
}

if (typeof document$ !== "undefined") {
  document$.subscribe(init);
} else {
  document.addEventListener("DOMContentLoaded", init);
}
