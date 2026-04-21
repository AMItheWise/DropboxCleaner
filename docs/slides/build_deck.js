"use strict";

const path = require("path");
const pptxgen = require("pptxgenjs");
const {
  warnIfSlideHasOverlaps,
  warnIfSlideElementsOutOfBounds,
} = require("./pptxgenjs_helpers/layout");

const pptx = new pptxgen();
pptx.layout = "LAYOUT_WIDE";
pptx.author = "Dropbox Cleaner contributors";
pptx.company = "Dropbox Cleaner";
pptx.subject = "Open-source project overview";
pptx.title = "Dropbox Cleaner";
pptx.lang = "en-US";
pptx.theme = {
  headFontFace: "Aptos Display",
  bodyFontFace: "Aptos",
  lang: "en-US",
};

const COLORS = {
  ink: "102A43",
  slate: "486581",
  blue: "147EFB",
  blueDark: "0F4C81",
  aqua: "2BB0ED",
  mint: "1F9D8B",
  gold: "D9A441",
  rose: "F06B6B",
  cloud: "F5F7FA",
  panel: "EAF2FF",
  white: "FFFFFF",
  line: "D9E2EC",
};

function addBackground(slide) {
  slide.background = { color: COLORS.white };
  slide.addShape(pptx.ShapeType.rect, {
    x: 0,
    y: 0,
    w: 13.333,
    h: 0.22,
    fill: { color: COLORS.blue },
    line: { color: COLORS.blue },
  });
}

function addTitle(slide, eyebrow, title, subtitle) {
  slide.addText(eyebrow, {
    x: 0.72,
    y: 0.48,
    w: 2.8,
    h: 0.22,
    fontFace: "Aptos",
    fontSize: 11,
    bold: true,
    color: COLORS.blue,
    charSpace: 0.4,
  });
  slide.addText(title, {
    x: 0.72,
    y: 0.78,
    w: 7.6,
    h: 0.56,
    fontFace: "Aptos Display",
    fontSize: 24,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  if (subtitle) {
    slide.addText(subtitle, {
      x: 0.72,
      y: 1.5,
      w: 7.9,
      h: 0.42,
      fontFace: "Aptos",
      fontSize: 11,
      color: COLORS.slate,
      valign: "mid",
      margin: 0,
    });
  }
}

function addFooter(slide, text) {
  slide.addText(text, {
    x: 0.72,
    y: 7.03,
    w: 5.8,
    h: 0.2,
    fontFace: "Aptos",
    fontSize: 8,
    color: COLORS.slate,
  });
}

function addPill(slide, x, y, w, label, color) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x,
    y,
    w,
    h: 0.34,
    rectRadius: 0.1,
    fill: { color },
    line: { color },
  });
  slide.addText(label, {
    x: x + 0.12,
    y: y + 0.05,
    w: w - 0.24,
    h: 0.2,
    fontFace: "Aptos",
    fontSize: 9,
    color: COLORS.white,
    bold: true,
    align: "center",
    margin: 0,
  });
}

function addCard(slide, x, y, w, h, title, body, accent) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x,
    y,
    w,
    h,
    rectRadius: 0.08,
    fill: { color: COLORS.cloud },
    line: { color: COLORS.line, pt: 1 },
  });
  slide.addShape(pptx.ShapeType.rect, {
    x,
    y,
    w: 0.08,
    h,
    fill: { color: accent },
    line: { color: accent },
  });
  slide.addText(title, {
    x: x + 0.22,
    y: y + 0.18,
    w: w - 0.34,
    h: 0.28,
    fontFace: "Aptos Display",
    fontSize: 14,
    bold: true,
    color: COLORS.ink,
    margin: 0,
  });
  slide.addText(body, {
    x: x + 0.22,
    y: y + 0.56,
    w: w - 0.34,
    h: h - 0.72,
    fontFace: "Aptos",
    fontSize: 10,
    color: COLORS.slate,
    valign: "top",
    margin: 0,
  });
}

function finalizeSlide(slide) {
  warnIfSlideHasOverlaps(slide, pptx);
  warnIfSlideElementsOutOfBounds(slide, pptx);
}

function coverSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);

  slide.addShape(pptx.ShapeType.roundRect, {
    x: 8.95,
    y: 0.78,
    w: 3.66,
    h: 5.72,
    rectRadius: 0.1,
    fill: { color: COLORS.blueDark },
    line: { color: COLORS.blueDark },
  });
  slide.addShape(pptx.ShapeType.arc, {
    x: 9.34,
    y: 1.22,
    w: 2.9,
    h: 2.9,
    line: { color: COLORS.aqua, pt: 2.2 },
    fill: { color: COLORS.blueDark, transparency: 100 },
    adjustPoint: 0.3,
  });
  slide.addShape(pptx.ShapeType.arc, {
    x: 9.56,
    y: 1.44,
    w: 2.45,
    h: 2.45,
    line: { color: COLORS.mint, pt: 2.2 },
    fill: { color: COLORS.blueDark, transparency: 100 },
    adjustPoint: 0.3,
  });
  slide.addText("Dropbox\nCleaner", {
    x: 0.72,
    y: 1.08,
    w: 5.8,
    h: 1.28,
    fontFace: "Aptos Display",
    fontSize: 28,
    bold: true,
    color: COLORS.ink,
    breakLine: false,
    margin: 0,
  });
  slide.addText("Safe staged archiving for personal Dropbox accounts", {
    x: 0.72,
    y: 2.48,
    w: 5.7,
    h: 0.56,
    fontFace: "Aptos",
    fontSize: 14,
    color: COLORS.slate,
    margin: 0,
  });
  slide.addText(
    "A local-first desktop utility that inventories Dropbox content, identifies older files, and stages archive copies into a dedicated Dropbox archive folder without deleting or moving originals.",
    {
      x: 0.72,
      y: 3.06,
      w: 6.6,
      h: 1.1,
      fontFace: "Aptos",
      fontSize: 12,
      color: COLORS.ink,
      margin: 0,
      valign: "top",
    }
  );

  addPill(slide, 0.72, 4.42, 1.68, "Local-first", COLORS.blue);
  addPill(slide, 2.52, 4.42, 1.96, "Copy-only", COLORS.mint);
  addPill(slide, 4.62, 4.42, 2.24, "Resume-safe", COLORS.gold);

  addCard(slide, 0.72, 5.12, 2.26, 1.16, "Desktop UX", "Simple GUI for non-technical users plus a shared CLI for advanced workflows.", COLORS.blue);
  addCard(slide, 3.14, 5.12, 2.26, 1.16, "Audit Trail", "CSV manifests, Markdown summaries, JSON outputs, and structured logs after every run.", COLORS.mint);
  addCard(slide, 5.56, 5.12, 2.26, 1.16, "Operational Safety", "Conflict-aware copy staging, checkpointed state, and Dropbox-side verification.", COLORS.gold);

  slide.addText("Designed for trustworthy archive staging, clean handoff, and open-source distribution.", {
    x: 9.38,
    y: 4.7,
    w: 2.8,
    h: 0.74,
    fontFace: "Aptos",
    fontSize: 12,
    color: COLORS.white,
    bold: true,
    align: "center",
    valign: "mid",
    margin: 0,
  });
  slide.addText("Python 3.11+\nPySide6 GUI\nCLI + shared services\nSQLite resumability", {
    x: 9.45,
    y: 5.5,
    w: 2.7,
    h: 0.75,
    fontFace: "Aptos",
    fontSize: 10,
    color: "D9F2FF",
    align: "center",
    margin: 0,
  });
  addFooter(slide, "Dropbox Cleaner • Open-source project overview");
  finalizeSlide(slide);
}

function workflowSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);
  addTitle(
    slide,
    "WORKFLOW",
    "Archive workflow users can trust",
    "Every run follows the same staged sequence so the operator can validate before Dropbox changes are made."
  );

  const steps = [
    ["1", "Inventory", "Enumerate full Dropbox metadata under one or more selected roots."],
    ["2", "Filter", "Select files older than the cutoff date and compute archive destinations."],
    ["3", "Plan", "Write manifests, summaries, and planned actions in dry-run mode."],
    ["4", "Stage", "Create the archive root and perform server-side copy operations only."],
    ["5", "Verify", "Compare matched sources against staged archive targets and report gaps."],
  ];
  let x = 0.72;
  for (let i = 0; i < steps.length; i += 1) {
    const [num, title, body] = steps[i];
    slide.addShape(pptx.ShapeType.roundRect, {
      x,
      y: 2.05,
      w: 2.1,
      h: 2.16,
      rectRadius: 0.08,
      fill: { color: COLORS.cloud },
      line: { color: COLORS.line, pt: 1 },
    });
    slide.addShape(pptx.ShapeType.ellipse, {
      x: x + 0.16,
      y: 2.16,
      w: 0.42,
      h: 0.42,
      fill: { color: COLORS.blue },
      line: { color: COLORS.blue },
    });
    slide.addText(num, {
      x: x + 0.16,
      y: 2.23,
      w: 0.42,
      h: 0.16,
      fontFace: "Aptos",
      fontSize: 9,
      bold: true,
      color: COLORS.white,
      align: "center",
      margin: 0,
    });
    slide.addText(title, {
      x: x + 0.16,
      y: 2.72,
      w: 1.78,
      h: 0.26,
      fontFace: "Aptos Display",
      fontSize: 15,
      bold: true,
      color: COLORS.ink,
      margin: 0,
    });
    slide.addText(body, {
      x: x + 0.16,
      y: 3.08,
      w: 1.78,
      h: 0.88,
      fontFace: "Aptos",
      fontSize: 9.6,
      color: COLORS.slate,
      margin: 0,
      valign: "top",
    });
    if (i < steps.length - 1) {
      slide.addShape(pptx.ShapeType.line, {
        x: x + 2.12,
        y: 3.14,
        w: 0.24,
        h: 0,
        line: { color: COLORS.blue, pt: 1.4, endArrowType: "triangle" },
      });
    }
    x += 2.38;
  }

  addCard(
    slide,
    0.78,
    4.78,
    4.0,
    1.28,
    "Why it feels safe",
    "Dry-run mode produces the same planning artifacts as a real run. The operator sees exactly what would be copied, where it would go, and whether Dropbox already contains matching or conflicting targets.",
    COLORS.mint
  );
  addCard(
    slide,
    4.94,
    4.78,
    3.4,
    1.28,
    "Why it scales",
    "The workflow streams inventory output, pages through Dropbox metadata, and checkpoints progress to SQLite so large accounts can resume cleanly after interruption.",
    COLORS.blue
  );
  addCard(
    slide,
    8.52,
    4.78,
    4.02,
    1.28,
    "Why it is practical",
    "The same backend powers the GUI and CLI, which keeps behavior consistent across non-technical and automation-friendly usage patterns.",
    COLORS.gold
  );

  addFooter(slide, "Staged archive flow • inventory → filter → plan → stage → verify");
  finalizeSlide(slide);
}

function uxSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);
  addTitle(
    slide,
    "DESKTOP EXPERIENCE",
    "A desktop utility that stays approachable",
    "The UI exposes only the decisions that matter: where to look, how old is old, where to stage copies, and whether the run is dry or real."
  );

  slide.addShape(pptx.ShapeType.roundRect, {
    x: 0.9,
    y: 2.0,
    w: 7.5,
    h: 4.35,
    rectRadius: 0.08,
    fill: { color: COLORS.white },
    line: { color: COLORS.line, pt: 1.2 },
  });
  slide.addShape(pptx.ShapeType.rect, {
    x: 0.9,
    y: 2.0,
    w: 7.5,
    h: 0.44,
    fill: { color: COLORS.cloud },
    line: { color: COLORS.cloud },
  });
  const tabs = ["Connection", "Job Setup", "Run / Progress", "Results"];
  let tabX = 1.08;
  tabs.forEach((tab, idx) => {
    slide.addText(tab, {
      x: tabX,
      y: 2.14,
      w: idx === 2 ? 1.25 : 1.0,
      h: 0.12,
      fontFace: "Aptos",
      fontSize: 9.2,
      bold: idx === 1,
      color: idx === 1 ? COLORS.ink : COLORS.slate,
      margin: 0,
    });
    tabX += idx === 2 ? 1.38 : 1.06;
  });
  slide.addShape(pptx.ShapeType.line, {
    x: 2.13,
    y: 2.38,
    w: 0.9,
    h: 0,
    line: { color: COLORS.blue, pt: 1.5 },
  });

  addCard(slide, 1.14, 2.72, 3.06, 1.42, "Connection", "OAuth PKCE, token storage, connection test, and account identity check in one place.", COLORS.blue);
  addCard(slide, 4.42, 2.72, 3.64, 1.42, "Job Setup", "Source roots, cutoff date, archive root, output directory, dry-run or copy mode, and advanced safety settings.", COLORS.mint);
  addCard(slide, 1.14, 4.34, 3.06, 1.54, "Run / Progress", "Phase label, live counters, real-time log output, and graceful cancellation while preserving run state.", COLORS.gold);
  addCard(slide, 4.42, 4.34, 3.64, 1.54, "Results", "Generated files, summary preview, conflict/failure preview, and resume access from the latest output folder.", COLORS.rose);

  addCard(
    slide,
    8.74,
    2.02,
    3.82,
    1.18,
    "Operator-friendly defaults",
    "Dry-run first, archive subtree excluded by default, and visible messaging that originals stay in place.",
    COLORS.blue
  );
  addCard(
    slide,
    8.74,
    3.42,
    3.82,
    1.18,
    "No terminal required",
    "A non-technical user can connect, configure, run, inspect artifacts, and resume work entirely from the desktop UI.",
    COLORS.mint
  );
  addCard(
    slide,
    8.74,
    4.82,
    3.82,
    1.18,
    "Same backend, two surfaces",
    "The GUI and CLI call the same orchestration layer, which keeps behavior consistent for demos, handoff, and scripted use.",
    COLORS.gold
  );

  addFooter(slide, "User-facing product surface • designed for clear operation and low-risk archive staging");
  finalizeSlide(slide);
}

function architectureSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);
  addTitle(
    slide,
    "ARCHITECTURE",
    "Clean separation between product surface and core workflow",
    "The project is structured for maintainability: thin interfaces at the edges, shared orchestration in the middle, and durable state underneath."
  );

  addCard(slide, 0.86, 2.12, 2.42, 1.12, "PySide6 GUI", "Guided connection, setup, progress, and results screens for local desktop operation.", COLORS.blue);
  addCard(slide, 0.86, 3.5, 2.42, 1.12, "CLI", "Advanced and automation-friendly entry points using the same services.", COLORS.mint);

  addShapeBlock(slide, 3.72, 2.62, 2.54, 1.4, "Run Orchestrator", "Coordinates inventory, filter, copy, verify, outputs", COLORS.blueDark, COLORS.white);
  addShapeBlock(slide, 6.62, 1.98, 2.48, 0.86, "Dropbox Adapter", "Auth, metadata listing, copy APIs", COLORS.panel, COLORS.ink);
  addShapeBlock(slide, 6.62, 3.0, 2.48, 0.86, "SQLite State", "Runs, checkpoints, copy jobs, events", COLORS.panel, COLORS.ink);
  addShapeBlock(slide, 6.62, 4.02, 2.48, 0.86, "Report Writers", "CSV, JSON, Markdown, JSONL", COLORS.panel, COLORS.ink);

  addShapeBlock(slide, 9.44, 2.12, 2.92, 0.94, "Dropbox API", "List folder, get metadata, create folder, copy", COLORS.cloud, COLORS.ink);
  addShapeBlock(slide, 9.44, 3.42, 2.92, 0.94, "Local Output Folder", "Run directories, manifests, logs, summaries", COLORS.cloud, COLORS.ink);
  addShapeBlock(slide, 9.44, 4.72, 2.92, 0.94, "Verification Layer", "Source vs staged archive comparison", COLORS.cloud, COLORS.ink);

  connector(slide, 3.28, 2.68, 0.44, 0);
  connector(slide, 3.28, 4.06, 0.44, 0);
  connector(slide, 6.26, 3.32, 0.36, 0);
  connector(slide, 9.1, 2.54, 0.34, 0);
  connector(slide, 9.1, 3.84, 0.34, 0);
  connector(slide, 9.1, 5.14, 0.34, 0);

  addCard(
    slide,
    0.86,
    5.82,
    5.4,
    0.82,
    "Why this matters",
    "Feature work stays concentrated in services and reports, while the UI and CLI remain thin shells around shared behavior.",
    COLORS.gold
  );
  addCard(
    slide,
    6.62,
    5.82,
    5.74,
    0.82,
    "Operational benefit",
    "Resumability, audit outputs, and Dropbox-side verification remain first-class concerns instead of being bolted on afterward.",
    COLORS.mint
  );

  addFooter(slide, "Project structure • product surface, shared orchestration, durable state, and external integrations");
  finalizeSlide(slide);
}

function addShapeBlock(slide, x, y, w, h, title, body, fillColor, textColor) {
  slide.addShape(pptx.ShapeType.roundRect, {
    x,
    y,
    w,
    h,
    rectRadius: 0.07,
    fill: { color: fillColor },
    line: { color: fillColor === COLORS.panel || fillColor === COLORS.cloud ? COLORS.line : fillColor, pt: 1 },
  });
  slide.addText(title, {
    x: x + 0.18,
    y: y + 0.14,
    w: w - 0.36,
    h: 0.22,
    fontFace: "Aptos Display",
    fontSize: 12.6,
    bold: true,
    color: textColor,
    margin: 0,
    align: "center",
  });
  slide.addText(body, {
    x: x + 0.18,
    y: y + 0.42,
    w: w - 0.36,
    h: h - 0.5,
    fontFace: "Aptos",
    fontSize: 9.1,
    color: textColor,
    margin: 0,
    align: "center",
    valign: "mid",
  });
}

function connector(slide, x, y, w, h) {
  slide.addShape(pptx.ShapeType.line, {
    x,
    y,
    w,
    h,
    line: { color: COLORS.blue, pt: 1.4, beginArrowType: "none", endArrowType: "triangle" },
  });
}

function trustSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);
  addTitle(
    slide,
    "SAFETY + AUDITABILITY",
    "Designed to create confidence before and after a run",
    "The strongest part of the product story is not just that it stages archive copies. It also tells the operator what happened, what was skipped, and what still needs attention."
  );

  addCard(slide, 0.84, 2.0, 2.36, 1.36, "Copy-first model", "Original Dropbox files remain untouched. The initial workflow only creates staged archive copies inside Dropbox.", COLORS.blue);
  addCard(slide, 3.46, 2.0, 2.36, 1.36, "Conflict-aware", "Existing archive targets are compared and classified as same or conflict instead of being overwritten silently.", COLORS.mint);
  addCard(slide, 6.08, 2.0, 2.36, 1.36, "Resume-safe", "Progress is written continuously to SQLite so already completed work is preserved after interruption.", COLORS.gold);
  addCard(slide, 8.7, 2.0, 3.78, 1.36, "Verification built in", "Matched source files are compared with staged archive targets so missing copies and conflicts stay visible.", COLORS.rose);

  addShapeBlock(slide, 0.84, 4.0, 5.32, 2.0, "Artifacts generated per run", "inventory_full.csv\nmatched_pre_cutoff.csv\nmanifest_dry_run.csv / manifest_copy_run.csv\nsummary.json + summary.md\nverification_report.csv + verification_report.json\napp.log + app.jsonl\nstate.db", COLORS.cloud, COLORS.ink);
  addShapeBlock(slide, 6.44, 4.0, 6.04, 2.0, "Why employers and clients care", "The product demonstrates careful systems thinking: explicit scope boundaries, durable state, deterministic outputs, and clear operational messaging that reduces user risk.", COLORS.panel, COLORS.ink);

  addFooter(slide, "Trust model • copy-first safety, resumability, conflict handling, and auditable run outputs");
  finalizeSlide(slide);
}

function repoSlide() {
  const slide = pptx.addSlide();
  addBackground(slide);
  addTitle(
    slide,
    "OPEN-SOURCE PACKAGE",
    "Ready for public review and contributor handoff",
    "The repository now ships with the assets expected from a polished public project, not just a code dump."
  );

  addCard(slide, 0.84, 2.02, 3.72, 1.22, "Project documentation", "Refined README, quick-start usage, public workflow summary, and slide deck previews for fast orientation.", COLORS.blue);
  addCard(slide, 4.82, 2.02, 3.72, 1.22, "Community health", "MIT license, contributing guide, code of conduct, security policy, changelog, issue templates, and PR template.", COLORS.mint);
  addCard(slide, 8.8, 2.02, 3.68, 1.22, "Release hygiene", "Git ignore rules, editor settings, package metadata, dev requirements, and CI validation for tests, compile checks, and build output.", COLORS.gold);

  slide.addShape(pptx.ShapeType.roundRect, {
    x: 0.84,
    y: 3.7,
    w: 6.06,
    h: 2.1,
    rectRadius: 0.07,
    fill: { color: COLORS.ink },
    line: { color: COLORS.ink },
  });
  slide.addText("Repository snapshot", {
    x: 1.08,
    y: 3.92,
    w: 1.9,
    h: 0.2,
    fontFace: "Aptos Display",
    fontSize: 13,
    bold: true,
    color: COLORS.white,
    margin: 0,
  });
  slide.addText(
    "app/\n  cli/\n  dropbox_client/\n  persistence/\n  reports/\n  services/\n  ui/\n.github/\ndocs/slides/\ntests/",
    {
      x: 1.08,
      y: 4.24,
      w: 5.36,
      h: 1.28,
      fontFace: "Courier New",
      fontSize: 11,
      color: "D9F2FF",
      margin: 0,
    }
  );

  addShapeBlock(slide, 7.22, 3.7, 5.26, 2.1, "Good public projects feel complete", "This repository is positioned to be reviewed by employers, collaborators, and open-source contributors without needing private context or handholding.", COLORS.cloud, COLORS.ink);

  addPill(slide, 0.84, 6.22, 2.14, "MIT licensed", COLORS.blue);
  addPill(slide, 3.14, 6.22, 2.48, "Cross-platform", COLORS.mint);
  addPill(slide, 5.82, 6.22, 3.06, "Tested + buildable", COLORS.gold);
  addPill(slide, 9.12, 6.22, 3.34, "Presentation included", COLORS.blueDark);

  addFooter(slide, "Public release posture • docs, governance, CI, and presentation assets included");
  finalizeSlide(slide);
}

async function main() {
  coverSlide();
  workflowSlide();
  uxSlide();
  architectureSlide();
  trustSlide();
  repoSlide();

  const outputPath = path.join(__dirname, "DropboxCleaner_Open_Source_Overview.pptx");
  await pptx.writeFile({ fileName: outputPath });
  console.log(`Wrote ${outputPath}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
