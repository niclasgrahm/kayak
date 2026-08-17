import { defineConfig } from 'vitepress'
import generated from './generated/sidebar.json' with { type: 'json' }

// The reference groups are *generated*: `docsgen` writes sidebar.json from the
// same reflection the pages themselves are built from, so a component added to
// the config enums turns up in the navigation without anyone editing this file.
// Everything else here is the prose, whose order is a judgement and is written.
const reference = (title: string, page: string, items: { text: string; link: string }[]) => ({
  text: title,
  collapsed: false,
  items: [{ text: 'overview', link: page }, ...items],
})

export default defineConfig({
  // The site is served as a *project page* — https://propell.dev/kayak/, since
  // propell.dev is the CNAME on the account's user site — so every generated
  // asset url needs the prefix. Getting this wrong is not subtle: the html
  // loads and every stylesheet and script under it 404s, so the page renders
  // unstyled rather than failing outright. This line goes the day kayak has a
  // domain of its own serving it from a root, and not before.
  base: '/kayak/',
  title: 'kayak',
  description: 'graph-based stream processing — configurable input → transforms → output pipelines, running on a live canvas',
  lang: 'en',
  cleanUrls: true,
  // this directory's own readme is for whoever edits the site, not a page of it
  srcExclude: ['readme.md'],
  lastUpdated: true,
  appearance: 'force-dark',
  head: [
    ['meta', { name: 'theme-color', content: '#1d2129' }],
    ['meta', { property: 'og:title', content: 'kayak' }],
    ['meta', {
      property: 'og:description',
      content: 'graph-based stream processing you can watch running',
    }],
  ],
  markdown: {
    theme: { light: 'github-light', dark: 'vitesse-dark' },
    // Shiki has no rhai grammar. Rust's is the closest thing that exists —
    // rhai is explicitly Rust-flavoured, and `let`, `fn`, `if`, `for … in`,
    // the comment syntax and the literals all land correctly. `#{ … }` is the
    // one thing it gets wrong, and a wrong brace beats every keyword falling
    // back to plain text. The fences still say `rhai`, which is what they are.
    languageAlias: { rhai: 'rust' },
  },
  themeConfig: {
    logo: undefined,
    siteTitle: 'kayak',
    outline: [2, 3],
    nav: [
      { text: 'guide', link: '/getting-started', activeMatch: '/(getting-started|canvas|pipelines|io|operating)/' },
      { text: 'reference', link: '/reference/', activeMatch: '/reference/' },
      { text: 'contributing', link: '/contributing/testing', activeMatch: '/contributing/' },
    ],
    sidebar: [
      {
        text: 'introduction',
        items: [
          { text: 'what kayak is', link: '/' },
          { text: 'getting started', link: '/getting-started' },
          { text: 'the config file', link: '/canvas/editing-the-graph#the-config-file' },
        ],
      },
      {
        text: 'the canvas',
        items: [
          { text: 'the canvas', link: '/canvas/the-canvas' },
          { text: 'editing the graph', link: '/canvas/editing-the-graph' },
          { text: 'arranging the canvas', link: '/canvas/arranging-the-canvas' },
        ],
      },
      {
        text: 'pipelines',
        items: [
          { text: 'the pipeline model', link: '/pipelines/pipelines' },
          { text: 'message metadata', link: '/pipelines/message-metadata' },
          { text: 'reshaping messages', link: '/pipelines/reshaping-messages' },
          { text: 'state', link: '/pipelines/state' },
          { text: 'scripting', link: '/pipelines/scripting' },
          { text: 'the sample graph', link: '/pipelines/the-sample' },
        ],
      },
      {
        text: 'getting data in and out',
        items: [
          { text: 'connections', link: '/io/connections' },
          { text: 'secrets', link: '/io/secrets' },
          { text: 'posting into a pipeline', link: '/io/posting-into-a-pipeline' },
          { text: 'sending over http', link: '/io/sending-over-http' },
          { text: 'opcua input', link: '/io/opcua-input' },
          { text: 'file output', link: '/io/file-output' },
          { text: 's3 output', link: '/io/s3-output' },
          { text: 'database outputs', link: '/io/database-outputs' },
        ],
      },
      {
        text: 'running a server',
        items: [
          { text: 'authentication', link: '/operating/authentication' },
          { text: 'history', link: '/operating/history' },
          { text: 'deployment', link: '/operating/deployment' },
        ],
      },
      {
        text: 'reference',
        items: [
          { text: 'how the reference is generated', link: '/reference/' },
          reference('inputs', '/reference/inputs', generated.inputs),
          reference('transforms', '/reference/transforms', generated.transforms),
          reference('outputs', '/reference/outputs', generated.outputs),
          reference('connections', '/reference/connections', generated.connections),
          { text: 'state buckets', link: '/reference/state' },
          reference('http api', '/reference/api', generated.api),
          { text: 'schemas', link: '/reference/schemas' },
        ],
      },
      {
        text: 'contributing',
        items: [
          { text: 'testing', link: '/contributing/testing' },
          { text: 'benchmarking', link: '/contributing/benchmarking' },
          { text: 'how the component reference works', link: '/contributing/how-the-component-reference-works' },
          { text: 'how the api reference works', link: '/contributing/how-the-api-reference-works' },
        ],
      },
    ],
    socialLinks: [
      { icon: 'github', link: 'https://github.com/niclasgrahm/kayak' },
    ],
    search: { provider: 'local' },
    editLink: {
      pattern: 'https://github.com/niclasgrahm/kayak/edit/main/website/:path',
      text: 'edit this page',
    },
    footer: {
      message: 'reference tables generated from the config schemas — <code>just docs</code>',
      copyright: 'kayak',
    },
  },
})
