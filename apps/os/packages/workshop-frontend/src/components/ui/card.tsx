import type { ComponentProps } from "react";
import { cn } from "@/lib/utils";

type CardProps = ComponentProps<"div">;

/** A token-aligned surface for management-page sections and resource previews. */
export function Card({ className, ...props }: CardProps) {
  return (
    <div
      data-slot="card"
      className={cn(
        "rounded-xl border border-kumo-line bg-kumo-base text-kumo-default shadow-sm",
        className,
      )}
      {...props}
    />
  );
}

/** The padded heading region of a {@link Card}. */
export function CardHeader({ className, ...props }: CardProps) {
  return (
    <div
      data-slot="card-header"
      className={cn("flex flex-col gap-1.5 p-5", className)}
      {...props}
    />
  );
}

/** A card heading. */
export function CardTitle({ className, ...props }: ComponentProps<"h3">) {
  return (
    <h3
      data-slot="card-title"
      className={cn(
        "text-sm font-medium tracking-[-0.15px] text-kumo-default",
        className,
      )}
      {...props}
    />
  );
}

/** Supporting text for a card heading. */
export function CardDescription({ className, ...props }: ComponentProps<"p">) {
  return (
    <p
      data-slot="card-description"
      className={cn("text-xs leading-5 text-kumo-subtle", className)}
      {...props}
    />
  );
}

/** The padded content region of a {@link Card}. */
export function CardContent({ className, ...props }: CardProps) {
  return (
    <div
      data-slot="card-content"
      className={cn("px-5 pb-5", className)}
      {...props}
    />
  );
}

/** The padded action region of a {@link Card}. */
export function CardFooter({ className, ...props }: CardProps) {
  return (
    <div
      data-slot="card-footer"
      className={cn("flex items-center gap-2 px-5 pb-5", className)}
      {...props}
    />
  );
}
