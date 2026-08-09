/**
 * Shared Monaco theme + monospace font stack used by both CodeEditor (full
 * editing surface) and CodeDiffEditor (read-mostly unified-diff renderer).
 *
 * Monaco renders into its own DOM that doesn't reliably inherit the page's
 * `--font-mono` CSS variable, so editor instances pass `monoFont` as an
 * explicit option. The CSS in CodeDiffEditor's view zones is in the same
 * boat — Monaco view zone DOM nodes don't pick up our body font either.
 *
 * The theme itself is shared because the diff editor and the regular editor
 * should look identical when displaying the same file; the diff editor only
 * adds line-level decorations on top.
 */

import type { ResolvedThemeMode } from '../theme'

export const VERGLAS_CODE_THEME_LIGHT = 'verglas-code-light'
export const VERGLAS_CODE_THEME_DARK = 'verglas-code-dark'

export const monoFont =
  '"IBM Plex Mono", ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace'

let themesDefined = false

export function getVesselsCodeTheme(theme: ResolvedThemeMode): string {
  return theme === 'dark' ? VERGLAS_CODE_THEME_DARK : VERGLAS_CODE_THEME_LIGHT
}

export function defineVesselsCodeTheme(monaco: typeof import('monaco-editor')): void {
  if (themesDefined) return

  monaco.editor.defineTheme(VERGLAS_CODE_THEME_LIGHT, {
    base: 'vs',
    inherit: true,
    rules: [
      { token: '', foreground: '121821' },
      { token: 'comment', foreground: '8b9bb0', fontStyle: 'italic' },
      { token: 'keyword', foreground: '2b87d9' },
      { token: 'storage', foreground: '2b87d9' },
      { token: 'operator', foreground: '5a6b80' },
      { token: 'string', foreground: '3d8f5c' },
      { token: 'number', foreground: '9a7a35' },
      { token: 'type', foreground: '0b6e6a' },
      { token: 'class', foreground: '0b6e6a' },
      { token: 'interface', foreground: '0b6e6a' },
      { token: 'function', foreground: '3d9cf0' },
      { token: 'variable', foreground: '121821' },
      { token: 'variable.predefined', foreground: '3d9cf0' },
      { token: 'constant', foreground: '9a7a35' },
      { token: 'delimiter', foreground: '5a6b80' },
      { token: 'tag', foreground: 'c94b54' },
      { token: 'attribute.name', foreground: '9a7a35' },
      { token: 'attribute.value', foreground: '3d8f5c' },
    ],
    colors: {
      'editor.background': '#f5f8fb',
      'editor.foreground': '#121821',
      'editorLineNumber.foreground': '#8b9bb0',
      'editorLineNumber.activeForeground': '#5a6b80',
      'editorCursor.foreground': '#121821',
      'editor.selectionBackground': '#d9ecfb',
      'editor.inactiveSelectionBackground': '#e4ebf3',
      'editor.selectionHighlightBackground': '#d9ecfb',
      'editor.wordHighlightBackground': '#e4ebf3',
      'editor.wordHighlightStrongBackground': '#d9ecfb',
      'editor.lineHighlightBackground': '#00000000',
      'editor.lineHighlightBorder': '#00000000',
      'editorGutter.background': '#f5f8fb',
      'editorIndentGuide.background1': '#d5e0eb',
      'editorIndentGuide.activeBackground1': '#c5d3e2',
      'editorWhitespace.foreground': '#d5e0eb',
      'editorOverviewRuler.border': '#00000000',
      'scrollbarSlider.background': '#9aafc433',
      'scrollbarSlider.hoverBackground': '#9aafc455',
      'scrollbarSlider.activeBackground': '#9aafc477',
    },
  })

  monaco.editor.defineTheme(VERGLAS_CODE_THEME_DARK, {
    base: 'vs-dark',
    inherit: true,
    rules: [
      { token: '', foreground: 'e8eef6' },
      { token: 'comment', foreground: '6b7c91', fontStyle: 'italic' },
      { token: 'keyword', foreground: '6bb4f5' },
      { token: 'storage', foreground: '6bb4f5' },
      { token: 'operator', foreground: '8b9bb0' },
      { token: 'string', foreground: '7fd99a' },
      { token: 'number', foreground: 'e6c07b' },
      { token: 'type', foreground: '5eead4' },
      { token: 'class', foreground: '5eead4' },
      { token: 'interface', foreground: '5eead4' },
      { token: 'function', foreground: '6bb4f5' },
      { token: 'variable', foreground: 'e8eef6' },
      { token: 'variable.predefined', foreground: '6bb4f5' },
      { token: 'constant', foreground: 'e6c07b' },
      { token: 'delimiter', foreground: '8b9bb0' },
      { token: 'tag', foreground: 'f07178' },
      { token: 'attribute.name', foreground: 'e6c07b' },
      { token: 'attribute.value', foreground: '7fd99a' },
    ],
    colors: {
      'editor.background': '#0b0f14',
      'editor.foreground': '#e8eef6',
      'editorLineNumber.foreground': '#6b7c91',
      'editorLineNumber.activeForeground': '#8b9bb0',
      'editorCursor.foreground': '#e8eef6',
      'editor.selectionBackground': '#1a3a55',
      'editor.inactiveSelectionBackground': '#153048',
      'editor.selectionHighlightBackground': '#153048',
      'editor.wordHighlightBackground': '#153048',
      'editor.wordHighlightStrongBackground': '#1a3a55',
      'editor.lineHighlightBackground': '#00000000',
      'editor.lineHighlightBorder': '#00000000',
      'editorGutter.background': '#0b0f14',
      'editorIndentGuide.background1': '#2a3544',
      'editorIndentGuide.activeBackground1': '#3d4d60',
      'editorWhitespace.foreground': '#2a3544',
      'editorOverviewRuler.border': '#00000000',
      'scrollbarSlider.background': '#2a354466',
      'scrollbarSlider.hoverBackground': '#3d4d6099',
      'scrollbarSlider.activeBackground': '#4a5d74cc',
    },
  })

  themesDefined = true
}
