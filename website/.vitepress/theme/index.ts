import DefaultTheme from 'vitepress/theme'
import type { Theme } from 'vitepress'
import Landing from './Landing.vue'
import './kayak.css'

// The landing page is a component rather than a `layout: home` frontmatter
// block: its middle section pins a pipeline card to the screen and changes
// what the card shows as the page scrolls, which no frontmatter can express.
export default {
  extends: DefaultTheme,
  enhanceApp({ app }) {
    app.component('Landing', Landing)
  },
} satisfies Theme
