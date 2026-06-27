interface PageHeaderProps {
  eyebrow: string
  title: string
  titleHighlight?: string
  subtitle: string
}

export function PageHeader({ eyebrow, title, titleHighlight, subtitle }: PageHeaderProps) {
  return (
    <div className="py-8">
      <div className="text-[10px] font-bold uppercase tracking-[0.2em] text-primary mb-2">
        {eyebrow}
      </div>
      <h1 className="text-2xl font-bold text-foreground tracking-tight mb-1">
        {title}
        {titleHighlight && (
          <span className="text-primary"> {titleHighlight}</span>
        )}
      </h1>
      <p className="text-sm text-muted-foreground">
        {subtitle}
      </p>
    </div>
  )
}


