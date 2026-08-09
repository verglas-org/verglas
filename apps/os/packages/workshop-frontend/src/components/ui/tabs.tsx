import {
  createContext,
  useCallback,
  useContext,
  useId,
  useState,
  type ComponentProps,
  type KeyboardEvent,
} from 'react'
import { cn } from '@/lib/utils'

type TabsContextValue = {
  baseId: string
  value: string
  setValue: (value: string) => void
}

const TabsContext = createContext<TabsContextValue | null>(null)

function useTabsContext(component: string) {
  const context = useContext(TabsContext)
  if (!context) throw new Error(`${component} must be used within Tabs`)
  return context
}

export type TabsProps = Omit<ComponentProps<'div'>, 'defaultValue'> & {
  value?: string
  defaultValue?: string
  onValueChange?: (value: string) => void
}

/**
 * Accessible, controlled or uncontrolled tabs with the usual shadcn composition API.
 *
 * Give every group a `defaultValue` (or a controlled `value`) and use matching values on
 * {@link TabsTrigger} and {@link TabsContent}.
 */
export function Tabs({ value: valueProp, defaultValue = '', onValueChange, className, children, ...props }: TabsProps) {
  const [uncontrolledValue, setUncontrolledValue] = useState(defaultValue)
  const baseId = useId()
  const value = valueProp ?? uncontrolledValue
  const setValue = useCallback((nextValue: string) => {
    if (valueProp === undefined) setUncontrolledValue(nextValue)
    onValueChange?.(nextValue)
  }, [onValueChange, valueProp])

  return (
    <TabsContext.Provider value={{ baseId, value, setValue }}>
      <div data-slot="tabs" className={cn('w-full', className)} {...props}>{children}</div>
    </TabsContext.Provider>
  )
}

/** The keyboard-navigable row of {@link TabsTrigger} elements. */
export function TabsList({ className, onKeyDown, ...props }: ComponentProps<'div'>) {
  const handleKeyDown = (event: KeyboardEvent<HTMLDivElement>) => {
    onKeyDown?.(event)
    if (event.defaultPrevented) return
    if (!['ArrowLeft', 'ArrowRight', 'Home', 'End'].includes(event.key)) return

    const triggers = Array.from(event.currentTarget.querySelectorAll<HTMLButtonElement>('[role="tab"]:not(:disabled)'))
    const currentIndex = triggers.indexOf(event.target as HTMLButtonElement)
    if (currentIndex < 0 || triggers.length === 0) return

    event.preventDefault()
    const nextIndex = event.key === 'Home'
      ? 0
      : event.key === 'End'
        ? triggers.length - 1
        : (currentIndex + (event.key === 'ArrowRight' ? 1 : -1) + triggers.length) % triggers.length
    triggers[nextIndex]?.click()
    triggers[nextIndex]?.focus()
  }

  return (
    <div
      data-slot="tabs-list"
      role="tablist"
      className={cn('inline-flex h-9 items-center gap-1 rounded-lg bg-kumo-elevated p-1 text-kumo-subtle', className)}
      onKeyDown={handleKeyDown}
      {...props}
    />
  )
}

export type TabsTriggerProps = ComponentProps<'button'> & { value: string }

/** A selectable tab control. */
export function TabsTrigger({ value, className, onClick, ...props }: TabsTriggerProps) {
  const { baseId, value: selectedValue, setValue } = useTabsContext('TabsTrigger')
  const selected = value === selectedValue
  return (
    <button
      data-slot="tabs-trigger"
      data-state={selected ? 'active' : 'inactive'}
      type="button"
      role="tab"
      id={`${baseId}-trigger-${value}`}
      aria-controls={`${baseId}-content-${value}`}
      aria-selected={selected}
      tabIndex={selected ? 0 : -1}
      className={cn(
        'inline-flex h-7 items-center justify-center rounded-md px-2.5 text-xs font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-kumo-ring/50 disabled:pointer-events-none disabled:opacity-50',
        selected ? 'bg-kumo-base text-kumo-default shadow-sm' : 'text-kumo-subtle hover:text-kumo-default',
        className,
      )}
      onClick={(event) => {
        onClick?.(event)
        if (!event.defaultPrevented) setValue(value)
      }}
      {...props}
    />
  )
}

export type TabsContentProps = ComponentProps<'div'> & { value: string; forceMount?: boolean }

/** Content associated with a {@link TabsTrigger}. Inactive content is unmounted by default. */
export function TabsContent({ value, forceMount = false, className, children, ...props }: TabsContentProps) {
  const { baseId, value: selectedValue } = useTabsContext('TabsContent')
  const selected = value === selectedValue
  if (!selected && !forceMount) return null
  return (
    <div
      data-slot="tabs-content"
      data-state={selected ? 'active' : 'inactive'}
      role="tabpanel"
      id={`${baseId}-content-${value}`}
      aria-labelledby={`${baseId}-trigger-${value}`}
      hidden={!selected}
      tabIndex={0}
      className={cn('mt-4 outline-none', className)}
      {...props}
    >
      {children}
    </div>
  )
}
