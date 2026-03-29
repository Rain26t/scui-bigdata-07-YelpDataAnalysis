"use strict";

const path = require("path");
const PptxGenJS = require("pptxgenjs");
const { imageSizingContain } = require("../pptxgenjs_helpers/image");
const {
  warnIfSlideHasOverlaps,
  warnIfSlideElementsOutOfBounds,
} = require("../pptxgenjs_helpers/layout");

const ROOT = path.resolve(__dirname, "..");
const ASSET = (...parts) => path.join(ROOT, "yelp_text_to_sql", ...parts);
const OUT = path.join(ROOT, "output", "SILKBYTE_X_QUERY_Startup_6_Slides.pptx");

const COLORS = {
  bg: "070B14",
  card: "111827",
  card2: "0E1628",
  ink: "F5F7FB",
  text: "DDE6F4",
  muted: "94A3B8",
  pink: "F472B6",
  cyan: "67E8F9",
  gold: "F6C56D",
  line: "22304B",
};

function addBackdrop(slide) {
  slide.background = { color: COLORS.bg };
  slide.addImage({
    path: ASSET("assets", "data_journey", "ppt", "design_blob_magenta.png"),
    x: 10.32,
    y: 0.1,
    w: 2.75,
    h: 2.75,
  });
  slide.addImage({
    path: ASSET("assets", "data_journey", "ppt", "design_blob_blue.png"),
    x: 0.08,
    y: 4.9,
    w: 2.5,
    h: 2.5,
  });
}

function addBrand(slide) {
  slide.addText("SILKBYTE X QUERY", {
    x: 0.72,
    y: 0.42,
    w: 2.6,
    h: 0.2,
    fontFace: "Aptos",
    fontSize: 11,
    bold: true,
    color: COLORS.cyan,
    charSpace: 1.4,
    margin: 0,
  });
}

function addFooter(slide, pageNo) {
  slide.addText(String(pageNo).padStart(2, "0"), {
    x: 12.15,
    y: 7.03,
    w: 0.35,
    h: 0.15,
    fontFace: "Aptos",
    fontSize: 9,
    color: COLORS.muted,
    align: "right",
    margin: 0,
  });
}

function addCard(slide, x, y, w, h, opts = {}) {
  slide.addShape("roundRect", {
    x,
    y,
    w,
    h,
    rectRadius: 0.12,
    fill: { color: opts.color || COLORS.card, transparency: opts.transparency ?? 0 },
    line: { color: opts.line || COLORS.line, pt: opts.pt || 1.1 },
    shadow: {
      type: "outer",
      color: "000000",
      angle: 45,
      blur: 2,
      distance: 1,
      opacity: 0.22,
    },
  });
}

function addTitle(slide, kicker, title, subtitle, opts = {}) {
  const x = opts.x ?? 0.8;
  const y = opts.y ?? 0.88;
  slide.addText(kicker.toUpperCase(), {
    x,
    y,
    w: opts.kickerW || 3.8,
    h: 0.18,
    fontFace: "Aptos",
    fontSize: 11,
    bold: true,
    color: COLORS.pink,
    charSpace: 1.1,
    margin: 0,
  });
  slide.addText(title, {
    x,
    y: y + 0.32,
    w: opts.titleW || 6.1,
    h: opts.titleH || 0.9,
    fontFace: "Aptos Display",
    fontSize: opts.titleSize || 24,
    bold: true,
    color: COLORS.ink,
    margin: 0,
    breakLine: false,
    fit: "shrink",
    valign: "mid",
  });
  slide.addText(subtitle, {
    x,
    y: y + (opts.subtitleOffsetY || 1.32),
    w: opts.subtitleW || 5.6,
    h: opts.subtitleH || 0.62,
    fontFace: "Aptos",
    fontSize: opts.subtitleSize || 12.5,
    color: COLORS.text,
    margin: 0,
    valign: "mid",
    breakLine: false,
    fit: "shrink",
  });
}

function addBullets(slide, bullets, x, y, w, h, fontSize = 14) {
  const runs = [];
  bullets.forEach((line, idx) => {
    runs.push({
      text: line,
      options: { bullet: { indent: 14 }, breakLine: true },
    });
    if (idx !== bullets.length - 1) {
      runs.push({ text: "", options: { breakLine: true } });
    }
  });
  slide.addText(runs, {
    x,
    y,
    w,
    h,
    fontFace: "Aptos",
    fontSize,
    color: COLORS.text,
    margin: 0,
    breakLine: false,
    valign: "top",
    paraSpaceAfterPt: 10,
  });
}

function addChip(slide, text, x, y, w) {
  slide.addShape("roundRect", {
    x,
    y,
    w,
    h: 0.34,
    rectRadius: 0.16,
    fill: { color: "2A1739", transparency: 2 },
    line: { color: "5B3C74", pt: 0.8 },
  });
  slide.addText(text, {
    x: x + 0.06,
    y: y + 0.075,
    w: w - 0.12,
    h: 0.14,
    fontFace: "Aptos",
    fontSize: 9.5,
    bold: true,
    color: COLORS.ink,
    align: "center",
    margin: 0,
  });
}

function addChipRow(slide, labels, x, y, maxW) {
  let cursorX = x;
  let cursorY = y;
  labels.forEach((label) => {
    const w = Math.min(Math.max(0.65 + label.length * 0.055, 1.0), 2.2);
    if (cursorX + w > x + maxW) {
      cursorX = x;
      cursorY += 0.42;
    }
    addChip(slide, label, cursorX, cursorY, w);
    cursorX += w + 0.1;
  });
}

function addImagePanel(slide, imgPath, x, y, w, h, caption) {
  addCard(slide, x, y, w, h, { color: COLORS.card2 });
  slide.addImage({
    path: imgPath,
    ...imageSizingContain(imgPath, x + 0.15, y + 0.15, w - 0.3, h - 0.62),
  });
  if (caption) {
    slide.addText(caption, {
      x: x + 0.18,
      y: y + h - 0.26,
      w: w - 0.36,
      h: 0.14,
      fontFace: "Aptos",
      fontSize: 8.5,
      color: COLORS.muted,
      align: "center",
      margin: 0,
    });
  }
}

function finalizeSlide(slide, pptx, pageNo) {
  addFooter(slide, pageNo);
  warnIfSlideHasOverlaps(slide, pptx, {
    muteContainment: true,
    ignoreDecorativeShapes: true,
  });
  warnIfSlideElementsOutOfBounds(slide, pptx);
}

async function main() {
  const pptx = new PptxGenJS();
  pptx.layout = "LAYOUT_WIDE";
  pptx.author = "Codex";
  pptx.company = "SilkByte X";
  pptx.subject = "Startup-style six-slide pitch";
  pptx.title = "SilkByte X Query - Startup Pitch";
  pptx.theme = {
    headFontFace: "Aptos Display",
    bodyFontFace: "Aptos",
    lang: "en-US",
  };

  const slides = [];

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addCard(slide, 0.56, 0.7, 12.1, 5.95);
    addTitle(
      slide,
      "Opening",
      "Where Big Data Infrastructure Meets Conversational Analytics",
      "SilkByte X Query transforms raw Yelp data into fast, explainable, decision-ready answers.",
      { x: 0.92, y: 1.15, titleW: 6.2, titleH: 1.22, titleSize: 26, subtitleW: 5.7 }
    );
    addChipRow(slide, ["Yelp Dataset", "Distributed Stack", "Explainable AI", "Startup-Ready Demo"], 0.94, 2.98, 5.8);
    slide.addText("From raw Yelp JSON to HDFS, Hive, PySpark, Zeppelin, and finally a polished Text-to-SQL product layer.", {
      x: 0.94,
      y: 3.82,
      w: 5.6,
      h: 1.0,
      fontFace: "Aptos",
      fontSize: 16,
      color: COLORS.text,
      margin: 0,
      valign: "mid",
      breakLine: false,
      fit: "shrink",
    });
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "closing_value_chain_v2.png"),
      7.08,
      1.05,
      5.0,
      4.75,
      "End-to-end value chain from storage to business action"
    );
    slide.addText("Simple question. Serious data system.", {
      x: 0.95,
      y: 5.7,
      w: 5.2,
      h: 0.3,
      fontFace: "Aptos",
      fontSize: 15,
      bold: true,
      color: COLORS.gold,
      margin: 0,
    });
    slides.push(slide);
  }

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addTitle(
      slide,
      "Project Vision",
      "A Full-Stack Analytics Product, Not Just A Class Project",
      "The vision was to connect infrastructure, analytics, and interface into one coherent experience.",
      { x: 0.82, y: 0.88, titleW: 6.35, subtitleW: 5.8 }
    );
    addChipRow(slide, ["System Thinking", "Product Framing", "AI Layer", "Business Utility"], 0.84, 2.5, 6.0);
    addCard(slide, 0.78, 3.12, 5.2, 3.33, { color: COLORS.card2 });
    addBullets(slide, [
      "Built as a connected workflow instead of isolated scripts and screenshots.",
      "Each layer has a clear job: ingest, structure, analyze, visualize, and converse.",
      "The final product makes sophisticated analysis accessible to non-technical stakeholders."
    ], 1.02, 3.44, 4.7, 2.4, 13.5);
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "product_transition_storyboard.png"),
      6.28,
      2.05,
      6.15,
      4.65,
      "Transition from analytics pipeline to product experience"
    );
    slides.push(slide);
  }

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addTitle(
      slide,
      "Why Yelp",
      "Why This Dataset Works For A Serious Product Story",
      "Yelp gives us real-world scale, rich entities, and business relevance in one public dataset.",
      { x: 0.82, y: 0.88, titleW: 6.1, subtitleW: 5.6 }
    );
    addChipRow(slide, ["6.6M Reviews", "192K Businesses", "200K Photos", "User + Review + Check-in"], 0.84, 2.5, 6.4);
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "yelp_dataset_stats.png"),
      0.78,
      3.05,
      5.45,
      3.78,
      "Scale and relevance make system validation credible"
    );
    addCard(slide, 6.48, 3.05, 5.85, 3.58, { color: COLORS.card2 });
    addBullets(slide, [
      "Combines merchant profiles, ratings, reviews, pictures, and credibility signals.",
      "Supports business analysis, user behavior analysis, sentiment mining, and check-in behavior.",
      "Feels immediately relevant to restaurants, local commerce, and market intelligence."
    ], 6.78, 3.37, 5.2, 2.2, 13.5);
    slides.push(slide);
  }

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addTitle(
      slide,
      "Architecture",
      "Three Layers. One Reliable Query Experience.",
      "The architecture balances usability, model intelligence, and safe data execution.",
      { x: 0.82, y: 0.88, titleW: 5.8, subtitleW: 5.4 }
    );
    addChipRow(slide, ["Presentation Layer", "Service Layer", "Data Layer", "Self-Correction"], 0.84, 2.5, 6.1);
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "query_architecture_blueprint.png"),
      0.74,
      3.02,
      7.3,
      3.95,
      "Layered architecture behind the application"
    );
    addCard(slide, 8.28, 3.02, 4.35, 3.68, { color: COLORS.card2 });
    addBullets(slide, [
      "UI captures intent and renders SQL, tables, and charts.",
      "Backend injects schema context, generates SQL, sanitizes it, and retries on failure.",
      "Data layer executes against Hive/Spark with real analytical structure."
    ], 8.56, 3.3, 3.75, 2.55, 12.5);
    slides.push(slide);
  }

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addTitle(
      slide,
      "From Analysis To Product",
      "The Breakthrough Was Productizing The Pipeline",
      "We moved beyond charts and notebooks by turning validated analysis into a user-facing product layer.",
      { x: 0.82, y: 0.88, titleW: 6.35, subtitleW: 5.95 }
    );
    addChipRow(slide, ["Zeppelin Evidence", "UI/UX Layer", "Operational Backend", "Demo-Ready"], 0.84, 2.5, 6.0);
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "product_transition_storyboard.png"),
      0.75,
      3.02,
      6.15,
      3.9,
      "Validated analytics became a product narrative"
    );
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "uiux_feature_stack.png"),
      7.04,
      3.02,
      5.55,
      3.9,
      "Conversation, SQL trace, tables, and charts in one flow"
    );
    slides.push(slide);
  }

  {
    const slide = pptx.addSlide();
    addBackdrop(slide);
    addBrand(slide);
    addTitle(
      slide,
      "Text-to-SQL Summary",
      "Ask In English. Execute In SQL. Return With Evidence.",
      "This is the product’s core moment: natural language becomes trusted analytics.",
      { x: 0.82, y: 0.88, titleW: 6.55, subtitleW: 5.55 }
    );
    addChipRow(slide, ["Schema Injection", "SQL Generation", "Sanitization", "Execution", "Charts"], 0.84, 2.5, 6.5);
    addImagePanel(
      slide,
      ASSET("assets", "data_journey", "generated", "text_to_sql_workflow_generated.png"),
      0.74,
      3.02,
      6.25,
      3.95,
      "Core orchestration from question to validated query"
    );
    addCard(slide, 7.18, 3.02, 5.42, 3.68, { color: COLORS.card2 });
    slide.addText("Top-rated Mexican restaurants in Philadelphia with 500+ reviews", {
      x: 7.46,
      y: 3.32,
      w: 4.85,
      h: 0.64,
      fontFace: "Aptos",
      fontSize: 14,
      bold: true,
      color: COLORS.gold,
      margin: 0,
      fit: "shrink",
    });
    addBullets(slide, [
      "Natural-language prompt enters the product.",
      "System injects live schema context before generation.",
      "Generated SQL is validated, executed, and returned with visuals."
    ], 7.44, 4.0, 4.78, 1.7, 12.5);
    slide.addText("Trusted answer, not black-box magic.", {
      x: 7.46,
      y: 5.92,
      w: 4.5,
      h: 0.24,
      fontFace: "Aptos",
      fontSize: 14,
      bold: true,
      color: COLORS.cyan,
      margin: 0,
    });
    slides.push(slide);
  }

  slides.forEach((slide, idx) => finalizeSlide(slide, pptx, idx + 1));

  await pptx.writeFile({ fileName: OUT });
  console.log(`Created: ${OUT}`);
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
