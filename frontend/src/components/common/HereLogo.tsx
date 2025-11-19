import hereLogoImg from '@/here-xy.jpg';

interface HereLogoProps {
  size?: 'sm' | 'md' | 'lg';
  className?: string;
}

export function HereLogo({ size = 'md', className = '' }: HereLogoProps) {
  const sizes = {
    sm: 'h-8',
    md: 'h-12',
    lg: 'h-16',
  };

  return (
    <img
      src={hereLogoImg}
      alt="HERE Technologies"
      className={`${sizes[size]} object-contain ${className}`}
    />
  );
}
