const menuButton = document.querySelector<HTMLButtonElement>('.menu-button')
const siteNavigation = document.querySelector<HTMLElement>('#site-nav')

menuButton?.addEventListener('click', () => {
  const open = menuButton.getAttribute('aria-expanded') !== 'true'
  menuButton.setAttribute('aria-expanded', String(open))
  siteNavigation?.setAttribute('data-open', String(open))
  menuButton.textContent = open ? 'Close' : 'Menu'
})

siteNavigation?.addEventListener('click', () => {
  menuButton?.setAttribute('aria-expanded', 'false')
  siteNavigation.setAttribute('data-open', 'false')
  if (menuButton) menuButton.textContent = 'Menu'
})

const copyButton = document.querySelector<HTMLButtonElement>('.copy-button')

copyButton?.addEventListener('click', async () => {
  const command = copyButton.dataset.copy
  if (!command) return

  try {
    await navigator.clipboard.writeText(command)
    copyButton.textContent = 'Copied'
  } catch {
    copyButton.textContent = 'Copy unavailable'
  }

  window.setTimeout(() => { copyButton.textContent = 'Copy command' }, 1800)
})

type GalleryView = {
  title: string
  copy: string
  label: string
  caption: string
  alt: string
  src: string
}

const galleryViews: Record<string, GalleryView> = {
  dashboard: {
    title: 'Live operations,<br><em>without the blind spot.</em>',
    copy: 'See running sources, materialized tables, pipelines, and the pressure points that need attention—on one operational canvas.',
    label: 'overview / live catalog',
    caption: 'Console overview: the live catalog at a glance.',
    alt: 'StreamsForge console overview showing platform activity.',
    src: '/media/product/dashboard.png',
  },
  pipeline: {
    title: 'SQL that<br><em>keeps answering.</em>',
    copy: 'Inspect a pipeline’s SQL, inputs, sink path and live throughput together, then follow each change back to the catalog.',
    label: 'pipeline / continuous query',
    caption: 'Pipeline detail: streaming SQL, input data and live execution status.',
    alt: 'StreamsForge pipeline detail with streaming SQL and source data.',
    src: '/media/product/pipeline.png',
  },
  table: {
    title: 'The current world,<br><em>materialized.</em>',
    copy: 'A table presents the latest live result, exposes its schema and provides a direct path to export or inspect the rows.',
    label: 'table / materialized result',
    caption: 'Table detail: the materialized, continuously updated result.',
    alt: 'StreamsForge materialized table with current rows and table controls.',
    src: '/media/product/table.png',
  },
  chat: {
    title: 'An operator can<br><em>ask the catalog.</em>',
    copy: 'AI Control uses function calling over the live catalog, so operational questions can become auditable actions instead of dashboard archaeology.',
    label: 'ai control / catalog actions',
    caption: 'AI Control: function calling over the StreamsForge catalog.',
    alt: 'StreamsForge AI Control conversation with catalog actions.',
    src: '/media/product/chat.png',
  },
}

const gallery = document.querySelector<HTMLElement>('[data-gallery]')
const galleryTitle = document.querySelector<HTMLElement>('[data-gallery-title]')
const galleryCopy = document.querySelector<HTMLElement>('[data-gallery-copy]')
const galleryLabel = document.querySelector<HTMLElement>('[data-gallery-label]')
const galleryCaption = document.querySelector<HTMLElement>('[data-gallery-caption]')
const galleryImage = document.querySelector<HTMLImageElement>('[data-gallery-image]')
const galleryControls = document.querySelectorAll<HTMLButtonElement>('[data-gallery-select]')

function showGalleryView(viewName: string) {
  const view = galleryViews[viewName]
  if (!view || !galleryImage || !gallery) return

  gallery.dataset.changing = 'true'
  window.setTimeout(() => {
    if (galleryTitle) galleryTitle.innerHTML = view.title
    if (galleryCopy) galleryCopy.textContent = view.copy
    if (galleryLabel) galleryLabel.textContent = view.label
    if (galleryCaption) galleryCaption.textContent = view.caption
    galleryImage.src = view.src
    galleryImage.alt = view.alt
    delete gallery.dataset.changing
  }, 140)

  galleryControls.forEach((control) => {
    const active = control.dataset.gallerySelect === viewName
    control.classList.toggle('is-active', active)
    control.setAttribute('aria-pressed', String(active))
  })
}

galleryControls.forEach((control) => {
  control.addEventListener('click', () => showGalleryView(control.dataset.gallerySelect ?? 'dashboard'))
})
