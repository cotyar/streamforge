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
