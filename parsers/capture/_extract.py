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
    if (bg && bg !== 'rgba(0, 0, 0, 0)' && bg !== 'transparent') {
      // Mark fills on interactive elements (hyperframes' interactiveBg signal) so
      // the brand action color wins the primary role by USAGE, not by saturation.
      const tag = el.tagName.toLowerCase();
      const role = (el.getAttribute('role') || '').toLowerCase();
      const cls = (el.className && el.className.toString) ? el.className.toString() : '';
      const interactive = tag === 'a' || tag === 'button' ||
        role === 'button' || role === 'link' || role === 'menuitem' || role === 'tab' ||
        /\b(btn|button|cta|primary|action)\b/i.test(cls);
      samples.push({color: bg, area: area, text: false, interactive: interactive});
    }
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

  const isVisible = (el) => {
    const s = getComputedStyle(el);
    return s.display !== 'none' && s.visibility !== 'hidden' && s.opacity !== '0'
      && el.getBoundingClientRect().height > 0;
  };
  // Caps below mirror tokenExtractor.ts: sections ≤30, per-section ctas ≤8,
  // assetUrls ≤10, section text ≤600; headings ≤40; svgs ≤50, outerHTML ≤10000.
  let sectionEls = Array.from(document.querySelectorAll('body > section, main > section, main > div, body > div'));
  sectionEls = sectionEls.filter(el => {
    const r = el.getBoundingClientRect();
    return r.height > 100 && r.width > 300;
  }).slice(0, 30);
  const sections = sectionEls.map((el, i) => {
    const r = el.getBoundingClientRect();
    const h = el.querySelector('h1,h2,h3');
    const headingText = h ? h.textContent.trim().slice(0,200) : '';
    // Inner content for faithful page-card recreation downstream (ports
    // tokenExtractor.ts section model): background-image, CTAs, in-section media
    // URLs, squeezed body text, and a coarse layout hint.
    const cs = getComputedStyle(el);
    let backgroundImage = '';
    const rawBgImg = cs.backgroundImage;
    if (rawBgImg && rawBgImg !== 'none' && rawBgImg.indexOf('url(') !== -1) {
      const start = rawBgImg.indexOf('url(') + 4;
      const end = rawBgImg.indexOf(')', start);
      if (end > start) backgroundImage = abs(rawBgImg.slice(start, end).replace(/["']/g, '')) || '';
    }
    const ctas = [];
    const ctaNodes = el.querySelectorAll('a, button');
    for (let qi = 0; qi < ctaNodes.length && ctas.length < 8; qi++) {
      if (!isVisible(ctaNodes[qi])) continue;
      const ct = (ctaNodes[qi].textContent || '').trim().replace(/\s+/g, ' ').slice(0, 60);
      if (ct && ct.length > 1 && ctas.indexOf(ct) === -1) ctas.push(ct);
    }
    const assetUrls = [];
    const mediaNodes = el.querySelectorAll('img, video, source');
    for (let ii = 0; ii < mediaNodes.length && assetUrls.length < 10; ii++) {
      const mn = mediaNodes[ii];
      const msrc = mn.currentSrc || mn.src || mn.getAttribute('src') ||
                   mn.getAttribute('data-src') || mn.getAttribute('poster') || '';
      const mau = abs(msrc);
      if (mau && !mau.startsWith('data:') && assetUrls.indexOf(mau) === -1) assetUrls.push(mau);
    }
    if (backgroundImage && !backgroundImage.startsWith('data:') &&
        assetUrls.indexOf(backgroundImage) === -1) assetUrls.unshift(backgroundImage);
    const imgCount = el.querySelectorAll('img').length;
    // Coarse layout hint; 'stacked' = default vertical flow (no image/heading cue).
    let layout = 'stacked';
    if (imgCount >= 3) layout = 'grid';
    else if (el.querySelector('img, video') && headingText) layout = 'split';
    else if (headingText && imgCount === 0) layout = 'centered';
    return {index: i, heading: headingText,
            classNames: (el.className||'').toString().split(/\s+/).filter(Boolean),
            bg: cs.backgroundColor,
            backgroundImage: backgroundImage,
            callsToAction: ctas,
            assetUrls: assetUrls,
            layout: layout,
            text: (el.innerText || el.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 600),
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

  // Headings — visible h1..h6, text squeezed to ~200 chars, cap 40.
  const headings = Array.from(document.querySelectorAll('h1,h2,h3,h4,h5,h6'))
    .filter(isVisible).slice(0, 40).map(h => {
      const s = getComputedStyle(h);
      return {level: parseInt(h.tagName[1]) || 1,
              text: (h.innerText || h.textContent || '').trim().replace(/\s+/g, ' ').slice(0, 200),
              fontSize: s.fontSize, fontWeight: s.fontWeight, color: s.color};
    });

  // SVGs — metadata + isLogo heuristic (ported from tokenExtractor.ts). outerHTML
  // stays here for later phases (SVG download) but is dropped before persisting.
  const titleBrand = (document.title || '').split(/[-|—]/)[0].trim();
  const svgs = Array.from(document.querySelectorAll('svg')).map(svg => {
    let label = svg.getAttribute('aria-label') || svg.getAttribute('title') || svg.getAttribute('alt') || '';
    if (!label) {
      const svgClasses = (svg.getAttribute('class') || '').split(/\s+/);
      const utilityPattern = /^(w-|h-|p-|m-|text-|bg-|border-|flex|grid|block|hidden|inline|absolute|relative|transition|duration|rotate|scale|opacity|group|sm:|md:|lg:|xl:)/;
      for (const cls of svgClasses) {
        if (cls.length > 3 && cls.length < 40 && !utilityPattern.test(cls) && cls !== 'lucide') { label = cls; break; }
      }
    }
    if (!label) {
      const svgId = svg.getAttribute('id') || '';
      if (svgId && svgId.length > 2 && svgId.length < 40) label = svgId;
    }
    if (!label) {
      const parent = svg.closest("[class*='icon'], [class*='logo'], [class*='nav'], [class*='btn'], [class*='social']");
      if (parent) {
        const parentClass = (parent.getAttribute('class') || '').split(' ').find(c => c.length > 3 && c.length < 30);
        if (parentClass) label = parentClass;
      }
    }
    if (!label) {
      const textEl = svg.querySelector('text');
      if (textEl && textEl.textContent && textEl.textContent.trim().length > 1 && textEl.textContent.trim().length < 30)
        label = textEl.textContent.trim();
    }
    const w = svg.getAttribute('width');
    const inLogoContext = svg.closest('[class*="logo"], [class*="brand"], [class*="partner"], [class*="customer"], [class*="marquee"]') !== null;
    if (!label && !inLogoContext && (!w || parseInt(w) < 16)) return null;
    let isLogo = (label && label.toLowerCase().indexOf('logo') !== -1) ||
                 svg.closest('[class*="logo"], [class*="brand"], [class*="home"], [class*="marquee"], [class*="partner"], [class*="customer"]') !== null;
    if (!isLogo) {
      const bannerEl = svg.closest('header, nav, [role="banner"]');
      if (bannerEl && bannerEl.querySelector('svg') === svg) isLogo = true;
    }
    if (!isLogo) {
      const anchor = svg.closest('a[href]');
      if (anchor) {
        const href = anchor.getAttribute('href') || '';
        if (href === '/' || href === '#' || href === './' || /^https?:\/\/[^/]+\/?$/.test(href)) isLogo = true;
      }
    }
    if (!isLogo) {
      const ariaLabel = svg.getAttribute('aria-label') || svg.getAttribute('title') || '';
      if (titleBrand.length > 1 && titleBrand.length < 30 &&
          ariaLabel.toLowerCase().indexOf(titleBrand.toLowerCase()) !== -1) isLogo = true;
    }
    const rect = svg.getBoundingClientRect();
    // Cheap metadata only; outerHTML (≤10000) attached AFTER filter+cap so we
    // never serialize markup for SVGs we discard (icon-heavy pages = hundreds).
    return {node: svg, label: label || '', viewBox: svg.getAttribute('viewBox') || '',
            width: Math.round(rect.width), height: Math.round(rect.height), isLogo: isLogo};
  }).filter(Boolean).slice(0, 50).map(s => ({
    label: s.label, viewBox: s.viewBox, width: s.width, height: s.height,
    outerHTML: s.node.outerHTML.slice(0, 10000), isLogo: s.isLogo}));

  const page = {width: Math.round(document.documentElement.scrollWidth),
                height: Math.round(document.documentElement.scrollHeight),
                viewport: {width: window.innerWidth, height: window.innerHeight}};

  return {final_url: location.href,
          colors: {samples, css_vars, theme_color},
          fonts: {display, body, face_srcs},
          spacing, sections, text, assets, motion,
          headings, svgs, page};
}
"""


async def run_extract(page) -> dict:
    """Run EXTRACT_JS in the page and return the raw dict."""
    return await page.evaluate(EXTRACT_JS)
