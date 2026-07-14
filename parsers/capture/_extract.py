"""Single-pass in-page extractor. JS collects; Python does the math."""
from __future__ import annotations

EXTRACT_JS = r"""
() => {
  const MAX_NODES = 4000;
  const abs = (u) => { try { return new URL(u, location.href).href; } catch(e){ return null; } };
  const els = Array.from(document.querySelectorAll('*')).slice(0, MAX_NODES);
  const samples = [];
  for (const el of els) {
    const r = el.getBoundingClientRect();
    const area = Math.max(0, r.width) * Math.max(0, r.height);
    if (area <= 0) continue;
    const cs = getComputedStyle(el);
    if (cs.display === 'none' || cs.visibility === 'hidden') continue;
    const bg = cs.backgroundColor;
    if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent')
      samples.push({color: bg, area: area, text: false});
    const hasText = el.childNodes && Array.from(el.childNodes)
      .some(n => n.nodeType === 3 && n.textContent.trim().length > 0);
    if (hasText) {
      const ta = Math.max(0, r.width) * Math.min(Math.max(0, r.height), 40);
      samples.push({color: cs.color, area: ta, text: true});
    }
  }
  const rootCS = getComputedStyle(document.documentElement);
  const css_vars = {};
  for (let i = 0; i < rootCS.length; i++) {
    const p = rootCS[i];
    if (p.startsWith('--') && /brand|primary|accent|bg|background|fg|text|color|main|theme/i.test(p)) {
      const v = rootCS.getPropertyValue(p).trim();
      if (v) css_vars[p] = v;
    }
  }
  const themeMeta = document.querySelector('meta[name="theme-color"]');
  const theme_color = themeMeta ? themeMeta.getAttribute('content') : null;

  const fontOf = (sel) => {
    const el = document.querySelector(sel);
    if (!el) return {};
    const cs = getComputedStyle(el);
    return {family: cs.fontFamily, weight: parseInt(cs.fontWeight) || 400, selector: sel};
  };
  const display = fontOf('h1').family ? fontOf('h1') : (fontOf('h2').family ? fontOf('h2') : fontOf('h3'));
  const body = fontOf('p').family ? fontOf('p') : fontOf('body');
  const face_srcs = {};
  for (const sheet of Array.from(document.styleSheets)) {
    let rules; try { rules = sheet.cssRules; } catch(e){ continue; }
    if (!rules) continue;
    for (const rule of Array.from(rules)) {
      if (rule.constructor && rule.constructor.name === 'CSSFontFaceRule') {
        const fam = (rule.style.getPropertyValue('font-family')||'').replace(/["']/g,'').trim().toLowerCase();
        const src = rule.style.getPropertyValue('src')||'';
        const urls = Array.from(src.matchAll(/url\(([^)]+)\)/g))
          .map(m => abs(m[1].replace(/["']/g,''))).filter(u => u && /woff2?/.test(u));
        if (fam && urls.length) (face_srcs[fam] = face_srcs[fam]||[]).push(...urls);
      }
    }
  }

  const nums = (arr) => arr.map(parseFloat).filter(x => !isNaN(x) && x > 0);
  const margins = [], paddings = [], radii = [];
  for (const el of els.slice(0, 800)) {
    const cs = getComputedStyle(el);
    margins.push(...nums([cs.marginTop, cs.marginBottom]));
    paddings.push(...nums([cs.paddingTop, cs.paddingBottom]));
    radii.push(...nums([cs.borderTopLeftRadius]));
  }
  let container_max_width = null;
  for (const el of els.slice(0, 800)) {
    const mw = parseFloat(getComputedStyle(el).maxWidth);
    if (!isNaN(mw) && mw > 400 && mw < 2000)
      container_max_width = Math.max(container_max_width||0, Math.round(mw));
  }
  const spacing = {margins, paddings, radii, container_max_width};

  let sectionEls = Array.from(document.querySelectorAll('body > section, main > section, main > div, body > div'));
  sectionEls = sectionEls.filter(el => {
    const r = el.getBoundingClientRect();
    return r.height > 100 && r.width > 300;
  }).slice(0, 30);
  const sections = sectionEls.map((el, i) => {
    const r = el.getBoundingClientRect();
    const h = el.querySelector('h1,h2,h3');
    return {index: i, heading: h ? h.textContent.trim().slice(0,200) : '',
            classNames: (el.className||'').toString().split(/\s+/).filter(Boolean),
            bg: getComputedStyle(el).backgroundColor,
            rect: {y: r.y + scrollY, height: r.height}};
  });
  const section_gaps = [];
  for (let i = 1; i < sections.length; i++)
    section_gaps.push(Math.abs(sections[i].rect.y - (sections[i-1].rect.y + sections[i-1].rect.height)));
  spacing.section_gaps = section_gaps;

  const meta = (n) => { const m = document.querySelector(n); return m ? (m.getAttribute('content')||'').trim() : ''; };
  const h1 = document.querySelector('h1');
  const text = {
    headline: meta('meta[property="og:title"]') || (h1 ? h1.textContent.trim() : document.title),
    tagline: meta('meta[name="description"]') || meta('meta[property="og:description"]'),
    ctas: Array.from(document.querySelectorAll('a,button'))
      .filter(a => /btn|cta|button/i.test(a.className) || a.tagName === 'BUTTON')
      .map(a => a.textContent.trim()).filter(t => t && t.length < 40).slice(0, 8),
    // All visible DOM text in reading order (matches hyperframes visible-text).
    // Capped so a huge page can't bloat the profile JSON column.
    full_text: (document.body ? document.body.innerText : '').slice(0, 100000),
  };

  const pickLogo = () => {
    const cand = document.querySelector('header img[alt*="logo" i], header svg, nav img[alt*="logo" i], [class*="logo" i] img, img[class*="logo" i]');
    if (cand && cand.tagName === 'IMG') return abs(cand.src);
    const apple = document.querySelector('link[rel="apple-touch-icon"]');
    if (apple) return abs(apple.href);
    return null;
  };
  const favicon = document.querySelector('link[rel~="icon"]');
  const video = document.querySelector('video source, video[src]');
  const lottie = document.querySelector('lottie-player[src], [data-animation-path]');
  const assets = {
    logo: pickLogo(),
    hero: meta('meta[property="og:image"]') || null,
    og_image: meta('meta[property="og:image"]') || null,
    favicon: favicon ? abs(favicon.href) : null,
    video: video ? abs(video.src || video.getAttribute('src')) : null,
    lottie: lottie ? abs(lottie.getAttribute('src') || lottie.getAttribute('data-animation-path')) : null,
  };

  const scriptsHref = Array.from(document.querySelectorAll('script[src], link[href]'))
    .map(s => (s.src || s.href || '')).join(' ').toLowerCase();
  const libs = [];
  for (const [lib, pat] of [['gsap','gsap'],['lottie','lottie'],['framer-motion','framer'],['aos','aos.'],['three','three']])
    if (scriptsHref.includes(pat)) libs.push(lib);
  const motion = {
    libraries: libs,
    has_video_background: !!document.querySelector('video[autoplay]'),
    has_canvas: !!document.querySelector('canvas'),
  };

  return {final_url: location.href,
          colors: {samples, css_vars, theme_color},
          fonts: {display, body, face_srcs},
          spacing, sections, text, assets, motion};
}
"""


async def run_extract(page) -> dict:
    """Run EXTRACT_JS in the page and return the raw dict."""
    return await page.evaluate(EXTRACT_JS)
