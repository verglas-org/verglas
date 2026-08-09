import { useState, useEffect, useRef, type ChangeEvent } from 'react'
import { RpcStub } from 'capnweb'
import { Switch, Textarea, Input, Button, Tabs, useKumoToastManager } from '@cloudflare/kumo'
import { Hexagon, ShieldWarning, UserPlus } from '@phosphor-icons/react'
import { useAuthenticatedApi } from './AuthContext'
import { AdminApi, AdminFormat, MAX_INSTANCE_INSTRUCTIONS_LENGTH, MAX_ANNOUNCEMENT_LENGTH, MAX_SITE_NAME_LENGTH, DEFAULT_SITE_NAME, BannerColor, BANNER_COLORS, DEFAULT_BANNER_COLOR } from '@verglas/workshop-shared/api'
import { applyAccentColor, DEFAULT_ACCENT_COLOR } from './theme'
import { cacheBustSiteLogoUrl, prepareSiteLogo } from './siteLogoUtils'
import SiteLogo from './components/SiteLogo'
import { useDocumentTitle } from './useDocumentTitle'
import AdminFormatsPanel from './components/format/AdminFormatsPanel'

// Preset accent colors offered in the Theme section ('' = Verglas primary #3d9cf0).
const ACCENT_PRESETS: { label: string; value: string }[] = [
  { label: 'Default', value: '' },
  { label: 'Ice', value: '#5eead4' },
  { label: 'Sky', value: '#60a5fa' },
  { label: 'Green', value: '#7fd99a' },
  { label: 'Amber', value: '#e6c07b' },
  { label: 'Rose', value: '#f07178' },
]

// Swatch background per banner color, matching AnnouncementBanner's accent styles.
const BANNER_SWATCH: Record<BannerColor, string> = {
  neutral: 'var(--color-kumo-tint)',
  info: 'var(--color-kumo-info)',
  success: 'var(--color-kumo-success)',
  warning: 'var(--color-kumo-warning)',
  danger: 'var(--color-kumo-danger)',
  brand: 'var(--color-accent-100)',
}

export default function AdminPage() {
  const { authenticatedApi, isAdmin } = useAuthenticatedApi()
  const toasts = useKumoToastManager()
  useDocumentTitle('Admin')

  // The admin capability (minted once via getAdminApi; null until loaded / for non-admins). Wrapped
  // in an object so useState doesn't treat the (callable) RPC stub as a state updater function.
  const [admin, setAdmin] = useState<{ api: RpcStub<AdminApi> } | null>(null)
  const [loading, setLoading] = useState(true)
  const [loadError, setLoadError] = useState(false)

  // System-prompt instructions: last-saved value + current editor draft.
  const [savedInstructions, setSavedInstructions] = useState('')
  const [instructionsDraft, setInstructionsDraft] = useState('')
  const [savingInstructions, setSavingInstructions] = useState(false)

  // Top-bar notice: last-saved value + current editor draft.
  const [savedAnnouncement, setSavedAnnouncement] = useState('')
  const [announcementDraft, setAnnouncementDraft] = useState('')
  const [savingAnnouncement, setSavingAnnouncement] = useState(false)

  // Full-width banner: last-saved value + current editor draft (text + accent color).
  const [savedBanner, setSavedBanner] = useState<{ text: string; color: BannerColor }>({ text: '', color: DEFAULT_BANNER_COLOR })
  const [bannerTextDraft, setBannerTextDraft] = useState('')
  const [bannerColorDraft, setBannerColorDraft] = useState<BannerColor>(DEFAULT_BANNER_COLOR)
  const [savingBanner, setSavingBanner] = useState(false)

  // Accent (brand) color: '' means the default theme. Live-previewed while editing.
  const [savedAccent, setSavedAccent] = useState('')
  const [accentDraft, setAccentDraft] = useState('')
  const [savingAccent, setSavingAccent] = useState(false)

  // Site name (shown next to the top-bar logo): last-saved value + current editor draft.
  const [savedSiteName, setSavedSiteName] = useState('')
  const [siteNameDraft, setSiteNameDraft] = useState('')
  const [savingSiteName, setSavingSiteName] = useState(false)

  // Current custom logo URL. Uploads are normalized to PNG before crossing the RPC boundary.
  const [siteLogoUrl, setSiteLogoUrl] = useState<string | null>(null)
  const [savingSiteLogo, setSavingSiteLogo] = useState(false)
  const siteLogoInputRef = useRef<HTMLInputElement>(null)

  // Whether new account signups are allowed.
  const [signupsEnabled, setSignupsEnabled] = useState(true)
  const [savingSignups, setSavingSignups] = useState(false)


  const [activeTab, setActiveTab] = useState('general')

  // Promoted output formats, in menu order (see AdminFormatsPanel).
  const [formats, setFormats] = useState<AdminFormat[]>([])


  // Populate all editor state from a freshly-fetched settings view.
  const applySettings = (view: Awaited<ReturnType<RpcStub<AdminApi>['getSettings']>>) => {
    setSignupsEnabled(view.signupsEnabled)
    setSavedSiteName(view.siteName)
    setSiteNameDraft(view.siteName)
    setSiteLogoUrl(view.siteLogo?.url ?? null)
    setSavedInstructions(view.instanceInstructions)
    setInstructionsDraft(view.instanceInstructions)
    setSavedAnnouncement(view.announcement)
    setAnnouncementDraft(view.announcement)
    setSavedBanner(view.banner)
    setBannerTextDraft(view.banner.text)
    setBannerColorDraft(view.banner.color)
    setSavedAccent(view.accentColor)
    setAccentDraft(view.accentColor)
    setFormats(view.formats)
  }

  // Mint the admin capability once (the access check happens server-side) and load settings.
  useEffect(() => {
    if (!isAdmin) {
      setLoading(false)
      return
    }
    let cancelled = false
    let stub: RpcStub<AdminApi> | null = null
    ;(async () => {
      try {
        const api = await authenticatedApi.getAdminApi()
        if (cancelled) {
          api?.[Symbol.dispose]?.()
          return
        }
        if (!api) {
          setLoadError(true)
          return
        }
        stub = api
        setAdmin({ api })
        applySettings(await api.getSettings())
      } catch (err) {
        if (!cancelled) {
          console.error('Failed to load admin settings:', err)
          setLoadError(true)
        }
      } finally {
        if (!cancelled) setLoading(false)
      }
    })()
    return () => {
      cancelled = true
      stub?.[Symbol.dispose]?.()
    }
  }, [isAdmin, authenticatedApi])

  // Live-preview the draft accent color across the whole app while the admin page is open. On leave
  // (or before each change) revert to the last-saved value so an unsaved preview doesn't stick.
  useEffect(() => {
    applyAccentColor(accentDraft)
    return () => { applyAccentColor(savedAccent) }
  }, [accentDraft, savedAccent])


  const handleSaveAnnouncement = async () => {
    if (!admin) return
    setSavingAnnouncement(true)
    try {
      await admin.api.setAnnouncement(announcementDraft)
      setSavedAnnouncement(announcementDraft)
      toasts.add({ title: 'Announcement saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save announcement'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingAnnouncement(false)
    }
  }

  const bannerDirty =
    bannerTextDraft !== savedBanner.text || bannerColorDraft !== savedBanner.color

  const handleSaveBanner = async () => {
    if (!admin) return
    setSavingBanner(true)
    try {
      await admin.api.setBanner(bannerTextDraft, bannerColorDraft)
      setSavedBanner({ text: bannerTextDraft, color: bannerColorDraft })
      toasts.add({ title: 'Banner saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save banner'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingBanner(false)
    }
  }

  const accentDirty = accentDraft !== savedAccent

  const handleSaveAccent = async () => {
    if (!admin) return
    setSavingAccent(true)
    try {
      await admin.api.setAccentColor(accentDraft)
      setSavedAccent(accentDraft)
      toasts.add({ title: 'Accent color saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save accent color'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingAccent(false)
    }
  }

  const handleSignupsToggle = async (enabled: boolean) => {
    if (!admin) return
    setSavingSignups(true)
    setSignupsEnabled(enabled) // optimistic
    try {
      await admin.api.setSignupsEnabled(enabled)
    } catch (err) {
      setSignupsEnabled(!enabled) // revert
      const message = err instanceof Error ? err.message : 'Update failed'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingSignups(false)
    }
  }

  const handleSaveSiteName = async () => {
    if (!admin) return
    setSavingSiteName(true)
    try {
      await admin.api.setSiteName(siteNameDraft)
      setSavedSiteName(siteNameDraft)
      toasts.add({ title: 'Site name saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save site name'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingSiteName(false)
    }
  }

  const handleSiteLogoChange = async (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file || !admin) return

    setSavingSiteLogo(true)
    try {
      const data = await prepareSiteLogo(file)
      const logo = await admin.api.setSiteLogo(data)
      setSiteLogoUrl(logo ? cacheBustSiteLogoUrl(logo.url) : null)
      toasts.add({ title: 'Logo saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save logo'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingSiteLogo(false)
    }
  }

  const handleRemoveSiteLogo = async () => {
    if (!admin) return
    setSavingSiteLogo(true)
    try {
      await admin.api.setSiteLogo(null)
      setSiteLogoUrl(null)
      toasts.add({ title: 'Default logo restored', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to remove logo'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingSiteLogo(false)
    }
  }

  const handleSaveInstructions = async () => {
    if (!admin) return
    setSavingInstructions(true)
    try {
      await admin.api.setInstanceInstructions(instructionsDraft)
      setSavedInstructions(instructionsDraft)
      toasts.add({ title: 'System prompt instructions saved', variant: 'success' })
    } catch (err) {
      const message = err instanceof Error ? err.message : 'Failed to save instructions'
      toasts.add({ title: message, variant: 'error' })
    } finally {
      setSavingInstructions(false)
    }
  }

  if (!isAdmin) {
    return (
      <div className="max-w-2xl mx-auto px-4 sm:px-6 py-16 text-center">
        <ShieldWarning size={32} className="mx-auto text-kumo-subtle mb-3" />
        <p className="text-sm text-kumo-default">You don't have access to this page.</p>
      </div>
    )
  }

  if (loading) {
    return (
      <div className="flex-1 flex items-center justify-center min-h-[60vh]">
        <p className="text-kumo-subtle">Loading admin settings...</p>
      </div>
    )
  }

  if (loadError || !admin) {
    return (
      <div className="mx-auto w-full max-w-[1040px] px-4 sm:px-8 py-16 text-center">
        <p className="text-sm text-kumo-danger">Something went wrong loading admin settings.</p>
        <button onClick={() => window.location.reload()} className="text-kumo-brand mt-2 text-sm underline">
          Try again
        </button>
      </div>
    )
  }

  return (
    <div className="mx-auto w-full max-w-[1040px] px-4 sm:px-8 py-8 space-y-6">
      <div>
        <h1 className="text-2xl font-semibold text-kumo-default">Admin</h1>
        <p className="text-sm text-kumo-subtle mt-1">
          Deployment-wide settings. Changes apply to all users on their next connection.
        </p>
      </div>

      <Tabs
        variant="underline"
        value={activeTab}
        onValueChange={setActiveTab}
        tabs={[
          { value: 'general', label: 'General' },
          { value: 'formats', label: 'Formats' },
          { value: 'access', label: 'Access' },
        ]}
      />

      {/* Standard output formats */}
      {activeTab === 'formats' && admin && (
        <AdminFormatsPanel
          admin={admin.api}
          formats={formats}
          onChanged={async () => { setFormats((await admin.api.getSettings()).formats) }}
        />
      )}

      {/* Sign-ups */}
      {activeTab === 'access' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <div className="flex items-center gap-4">
            <div className="w-9 h-9 rounded-lg flex-shrink-0 flex items-center justify-center bg-kumo-tint">
              <UserPlus size={18} className="text-kumo-subtle" />
            </div>
            <div className="flex-1 min-w-0">
              <h2 className="text-lg font-semibold text-kumo-strong">Allow new sign-ups</h2>
              <p className="text-sm text-kumo-subtle mt-0.5">
                When off, existing users can still log in but no new accounts can be created.
              </p>
            </div>
            <Switch
              checked={signupsEnabled}
              disabled={savingSignups}
              onCheckedChange={handleSignupsToggle}
            />
          </div>
        </div>
      )}

      {/* Site name */}
      {activeTab === 'general' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <h2 className="text-lg font-semibold text-kumo-strong mb-1">Site name</h2>
          <p className="text-sm text-kumo-subtle mb-5">
            Shown next to the logo in the top bar. Leave empty to use the default
            (&ldquo;{DEFAULT_SITE_NAME}&rdquo;). Applies on each user&rsquo;s next connection.
          </p>

          <Input
            value={siteNameDraft}
            onChange={(e) => setSiteNameDraft(e.target.value)}
            placeholder={DEFAULT_SITE_NAME}
            maxLength={MAX_SITE_NAME_LENGTH}
          />

          <div className="flex items-center justify-end mt-4 gap-2">
            {siteNameDraft !== savedSiteName && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setSiteNameDraft(savedSiteName)}
                disabled={savingSiteName}
              >
                Reset
              </Button>
            )}
            <Button
              variant="primary"
              size="sm"
              onClick={handleSaveSiteName}
              loading={savingSiteName}
              disabled={siteNameDraft === savedSiteName}
            >
              Save
            </Button>
          </div>
        </div>
      )}

      {/* Site logo */}
      {activeTab === 'general' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <h2 className="text-lg font-semibold text-kumo-strong mb-1">Logo</h2>
          <p className="text-sm text-kumo-subtle mb-5">
            Shown in the app chrome, sign-in screens, and browser tab. Images are scaled without
            cropping and converted to a static PNG. Square images work best. Applies on each
            user&rsquo;s next connection.
          </p>

          <div className="flex flex-wrap items-center gap-4">
            <div className="flex h-16 w-16 items-center justify-center rounded-xl border border-kumo-line bg-kumo-base p-2">
              <SiteLogo size={40} srcOverride={siteLogoUrl}>
                <Hexagon size={32} weight="bold" className="text-kumo-brand" />
              </SiteLogo>
            </div>
            <input
              ref={siteLogoInputRef}
              type="file"
              accept="image/png,image/jpeg,image/webp,image/svg+xml"
              className="hidden"
              disabled={savingSiteLogo}
              onChange={handleSiteLogoChange}
            />
            <div className="flex items-center gap-2">
              <Button
                variant="secondary"
                size="sm"
                onClick={() => siteLogoInputRef.current?.click()}
                loading={savingSiteLogo}
                disabled={savingSiteLogo}
              >
                {siteLogoUrl ? 'Change logo' : 'Upload logo'}
              </Button>
              {siteLogoUrl && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={handleRemoveSiteLogo}
                  disabled={savingSiteLogo}
                >
                  Restore default
                </Button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Theme / accent color */}
      {activeTab === 'general' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <h2 className="text-lg font-semibold text-kumo-strong mb-1">Theme</h2>
          <p className="text-sm text-kumo-subtle mb-5">
            Accent color used for buttons, links, and highlights. Changes preview live here; click
            Save to apply for everyone (on their next connection). Backgrounds keep the default
            warm theme.
          </p>

          <div className="flex flex-wrap items-center gap-2 mb-4">
            {ACCENT_PRESETS.map((preset) => {
              const selected = accentDraft === preset.value
              const swatch = preset.value || DEFAULT_ACCENT_COLOR
              return (
                <button
                  key={preset.label}
                  type="button"
                  onClick={() => setAccentDraft(preset.value)}
                  className={`flex items-center gap-2 pl-1.5 pr-3 py-1.5 rounded-full border text-xs font-medium transition-colors ${
                    selected
                      ? 'border-kumo-default text-kumo-default bg-kumo-tint'
                      : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'
                  }`}
                >
                  <span
                    className="w-4 h-4 rounded-full border border-kumo-line"
                    style={{ background: swatch }}
                  />
                  {preset.label}
                </button>
              )
            })}
          </div>

          <div className="flex items-center gap-3">
            <label className="flex items-center gap-2 text-sm text-kumo-default cursor-pointer">
              <input
                type="color"
                value={accentDraft || DEFAULT_ACCENT_COLOR}
                onChange={(e) => setAccentDraft(e.target.value)}
                className="w-9 h-9 rounded-md border border-kumo-line bg-transparent cursor-pointer p-0.5"
              />
              Custom
            </label>
            <span className="text-xs font-mono text-kumo-subtle">
              {accentDraft || `${DEFAULT_ACCENT_COLOR} (default)`}
            </span>
            <div className="flex-1" />
            {accentDirty && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setAccentDraft(savedAccent)}
                disabled={savingAccent}
              >
                Reset
              </Button>
            )}
            <Button
              variant="primary"
              size="sm"
              onClick={handleSaveAccent}
              loading={savingAccent}
              disabled={!accentDirty}
            >
              Save
            </Button>
          </div>
        </div>
      )}

      {/* Full-width banner */}
      {activeTab === 'general' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <h2 className="text-lg font-semibold text-kumo-strong mb-1">Banner</h2>
          <p className="text-sm text-kumo-subtle mb-5">
            A dismissible bar across the very top of the app (logged in or not). Markdown is
            supported, so you can include links. Leave empty to hide it. Applies on each
            user&rsquo;s next connection.
          </p>

          <Textarea
            className="w-full"
            value={bannerTextDraft}
            onValueChange={setBannerTextDraft}
            rows={1}
            placeholder={'e.g. \uD83C\uDF89 New: blueprints now support imports \u2014 [learn more](https://example.com).'}
            maxLength={MAX_ANNOUNCEMENT_LENGTH}
            error={
              bannerTextDraft.length > MAX_ANNOUNCEMENT_LENGTH
                ? `Too long by ${bannerTextDraft.length - MAX_ANNOUNCEMENT_LENGTH} characters`
                : undefined
            }
          />

          <div className="mt-4 flex items-end justify-between gap-4">
            <div className="min-w-0">
              <p className="text-xs font-medium text-kumo-subtle mb-2">Type</p>
              <div className="flex flex-wrap items-center gap-2">
                {BANNER_COLORS.map((c) => {
                  const selected = bannerColorDraft === c
                  return (
                    <button
                      key={c}
                      type="button"
                      onClick={() => setBannerColorDraft(c)}
                      className={`flex items-center gap-2 pl-1.5 pr-3 py-1.5 rounded-full border text-xs font-medium transition-colors ${
                        selected
                          ? 'border-kumo-default text-kumo-default bg-kumo-tint'
                          : 'border-kumo-line text-kumo-subtle hover:bg-kumo-tint'
                      }`}
                    >
                      <span
                        className="w-4 h-4 rounded-full border border-kumo-line"
                        style={{ background: BANNER_SWATCH[c] }}
                      />
                      {c.charAt(0).toUpperCase() + c.slice(1)}
                    </button>
                  )
                })}
              </div>
            </div>

            <div className="flex items-center gap-2 flex-shrink-0">
              {bannerDirty && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => {
                    setBannerTextDraft(savedBanner.text)
                    setBannerColorDraft(savedBanner.color)
                  }}
                  disabled={savingBanner}
                >
                  Reset
                </Button>
              )}
              <Button
                variant="primary"
                size="sm"
                onClick={handleSaveBanner}
                loading={savingBanner}
                disabled={!bannerDirty || bannerTextDraft.length > MAX_ANNOUNCEMENT_LENGTH}
              >
                Save
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Top-bar notice */}
      {activeTab === 'general' && (
        <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
          <h2 className="text-lg font-semibold text-kumo-strong mb-1">Top-bar notice</h2>
          <p className="text-sm text-kumo-subtle mb-5">
            Shown centered in the top navigation bar. Markdown is supported, so you can include
            links. Keep it short — it renders on a single line. Leave empty to show nothing. Applies
            on each user&rsquo;s next connection.
          </p>

          <Textarea
            className="w-full"
            value={announcementDraft}
            onValueChange={setAnnouncementDraft}
            rows={1}
            placeholder={'e.g. Heads up: scheduled maintenance Saturday \u2014 see [status](https://status.example.com).'}
            maxLength={MAX_ANNOUNCEMENT_LENGTH}
            error={
              announcementDraft.length > MAX_ANNOUNCEMENT_LENGTH
                ? `Too long by ${announcementDraft.length - MAX_ANNOUNCEMENT_LENGTH} characters`
                : undefined
            }
          />

          <div className="flex items-center justify-between mt-3">
            <span className="text-xs text-kumo-subtle">
              {announcementDraft.length.toLocaleString()} / {MAX_ANNOUNCEMENT_LENGTH.toLocaleString()} characters
            </span>
            <div className="flex items-center gap-2">
              {announcementDraft !== savedAnnouncement && (
                <Button
                  variant="ghost"
                  size="sm"
                  onClick={() => setAnnouncementDraft(savedAnnouncement)}
                  disabled={savingAnnouncement}
                >
                  Reset
                </Button>
              )}
              <Button
                variant="primary"
                size="sm"
                onClick={handleSaveAnnouncement}
                loading={savingAnnouncement}
                disabled={
                  announcementDraft === savedAnnouncement ||
                  announcementDraft.length > MAX_ANNOUNCEMENT_LENGTH
                }
              >
                Save
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Agent system prompt additions */}
      {activeTab === 'general' && (
      <div className="bg-kumo-elevated border border-kumo-line rounded-xl p-6">
        <h2 className="text-lg font-semibold text-kumo-strong mb-1">Agent instructions</h2>
        <p className="text-sm text-kumo-subtle mb-5">
          Extra instructions added to every agent&rsquo;s system prompt on this deployment. Use this
          for instance-specific context, conventions, or guardrails.
        </p>

        <Textarea
          className="w-full"
          value={instructionsDraft}
          onValueChange={setInstructionsDraft}
          rows={6}
          placeholder={'e.g. ACME Corp is a logistics company that helps small businesses ship\ninternationally. Our team builds internal tools and dashboards to track shipments.'}
          maxLength={MAX_INSTANCE_INSTRUCTIONS_LENGTH}
          error={
            instructionsDraft.length > MAX_INSTANCE_INSTRUCTIONS_LENGTH
              ? `Too long by ${instructionsDraft.length - MAX_INSTANCE_INSTRUCTIONS_LENGTH} characters`
              : undefined
          }
        />

        <div className="flex items-center justify-between mt-3">
          <span className="text-xs text-kumo-subtle">
            {instructionsDraft.length.toLocaleString()} / {MAX_INSTANCE_INSTRUCTIONS_LENGTH.toLocaleString()} characters
          </span>
          <div className="flex items-center gap-2">
            {instructionsDraft !== savedInstructions && (
              <Button
                variant="ghost"
                size="sm"
                onClick={() => setInstructionsDraft(savedInstructions)}
                disabled={savingInstructions}
              >
                Reset
              </Button>
            )}
            <Button
              variant="primary"
              size="sm"
              onClick={handleSaveInstructions}
              loading={savingInstructions}
              disabled={
                instructionsDraft === savedInstructions ||
                instructionsDraft.length > MAX_INSTANCE_INSTRUCTIONS_LENGTH
              }
            >
              Save
            </Button>
          </div>
        </div>
      </div>
      )}
    </div>
  )
}
