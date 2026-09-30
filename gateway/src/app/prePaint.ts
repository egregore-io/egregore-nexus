// Applied before first paint (inline in the root <head>) so a remembered
// collapse/appearance state shows with no flash. Reads the raw `nexus.ui`
// localStorage blob (zustand persist format: { state: {...}, version }). Pure
// string — no imports; runs in the browser only.
export const PRE_PAINT_SRC = `(function(){try{
  var raw = localStorage.getItem('nexus.ui'); if(!raw) return;
  var s = (JSON.parse(raw)||{}).state||{}; var d = document.documentElement;
  var c = s.collapsed||{};
  if(c.rail) d.dataset.rail='collapsed';
  if(c.ctx) d.dataset.ctx='collapsed';
  if(c.search===false) d.dataset.search='open';
  if(s.accent) d.dataset.accent=s.accent;
  if(typeof s.backdropDim==='number') d.style.setProperty('--lens-backdrop-dim', String(s.backdropDim));
}catch(e){}})();`;
