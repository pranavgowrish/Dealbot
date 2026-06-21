import { AMBIENT_LISTINGS } from '../data/ambientListings';

type ListingAmbientBackgroundProps = {
  subdued?: boolean;
};

const TRACKS = [
  { top: '6%', duration: 80, reverse: false, start: 0 },
  { top: '22%', duration: 105, reverse: true, start: 4 },
  { top: '38%', duration: 92, reverse: false, start: 8 },
  { top: '54%', duration: 118, reverse: true, start: 12 },
  { top: '70%', duration: 88, reverse: false, start: 16 },
  { top: '86%', duration: 112, reverse: true, start: 2 },
] as const;

function trackItems(start: number) {
  const rotated = [
    ...AMBIENT_LISTINGS.slice(start),
    ...AMBIENT_LISTINGS.slice(0, start),
  ];
  return [...rotated, ...rotated];
}

function AmbientListingCard({
  title,
  price,
  source,
}: {
  title: string;
  price: string;
  source: string;
}) {
  return (
    <div className="ambient-listing-card">
      <div className="ambient-listing-thumb">
        <span className="ambient-listing-thumb-icon" aria-hidden>
          📷
        </span>
      </div>
      <div className="ambient-listing-body">
        <p className="ambient-listing-title">{title}</p>
        <div className="ambient-listing-meta">
          <span className="ambient-listing-price">{price}</span>
          <span className="ambient-listing-source">{source}</span>
        </div>
      </div>
    </div>
  );
}

/** Generic listing ticker rows — decorative background only. */
export function ListingAmbientBackground({
  subdued = false,
}: ListingAmbientBackgroundProps) {
  return (
    <div
      aria-hidden
      className={`listing-ambient-bg ${subdued ? 'listing-ambient-bg--subdued' : ''}`}
    >
      {TRACKS.map((track) => (
        <div
          key={track.top}
          className={`listing-ambient-track ${track.reverse ? 'listing-ambient-track--reverse' : ''}`}
          style={{
            top: track.top,
            animationDuration: `${track.duration}s`,
          }}
        >
          {trackItems(track.start).map((listing, index) => (
            <AmbientListingCard
              key={`${track.top}-${listing.title}-${index}`}
              title={listing.title}
              price={listing.price}
              source={listing.source}
            />
          ))}
        </div>
      ))}
    </div>
  );
}
