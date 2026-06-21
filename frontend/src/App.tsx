/* ==========================================================================
   App.tsx — One-flow landing interface (root view)
   Flow: idle → fastapi_loading → show_listings → success_confirmed
   ========================================================================== */

import { useEffect, useRef, useState, type FormEvent } from 'react';
import { AnimatePresence, motion } from 'framer-motion';
import { JourneyStepper } from './components/JourneyStepper';
import { ListingAmbientBackground } from './components/ListingAmbientBackground';
import { LoadingProgress } from './components/LoadingProgress';
import { useAgentStream } from './hooks/useAgentStream';

const LOGO_SRC = '/LOGO_SRC.png';
/** UI timing only — swap to real API await when wiring backend response. */
const FASTAPI_LOADING_MS = 10_000;
/** Business rule — max picks before confirm. Change if product allows more/fewer. */
const MAX_LISTING_SELECTIONS = 5;
/** Must match backend listing count (currently 10 in main.py). */
const MOCK_LISTING_COUNT = 10;

/** HARDCODED UI — replace with dynamic suggestions from search history or API. */
const PRODUCT_CHIPS = ['Couch', 'iPhone 14 Pro', 'Electric guitar', 'Office chair'];
/** HARDCODED UI — replace with slider or parsed max budget from user profile. */
const BUDGET_CHIPS = ['$300', '$600', '$1000', '$2000'];

/** HARDCODED UI — delete when listings come from POST /api/v1/orchestrate response. */
const MOCK_TITLES = [
  'Vintage leather sectional — great condition',
  'iPhone 14 Pro 256GB — unlocked',
  'Mid-century modern desk chair',
  'Like-new road bike — barely used',
  'Sony WH-1000XM5 headphones',
  'Standing desk with drawer',
  'Nintendo Switch OLED bundle',
  'West Elm dining table',
  'MacBook Air M2 — 16GB RAM',
  'Patagonia down jacket — size M',
];

type LoadingState =
  | 'idle'
  | 'fastapi_loading'
  | 'show_listings'
  | 'success_confirmed';

/** HARDCODED UI — replace with `listings` state populated from fetch response in handleSubmit. */
const MOCK_LISTINGS = Array.from({ length: MOCK_LISTING_COUNT }, (_, i) => ({
  id: i + 1,
  title: MOCK_TITLES[i] ?? `Listing ${i + 1}`,
  price: `$${(380 + i * 47).toLocaleString()}`, // TODO: use listing.price from API
  area: ['LA', 'Pasadena', 'Glendale', 'Burbank', 'Santa Monica'][i % 5], // TODO: use listing.location from API
  source: ['Facebook', 'Craigslist', 'OfferUp', 'Mercari', 'Nextdoor'][i % 5], // TODO: use listing.source from API
}));

function journeyIndex(state: LoadingState): number {
  switch (state) {
    case 'idle':
      return 0;
    case 'fastapi_loading':
      return 1;
    case 'show_listings':
      return 2;
    case 'success_confirmed':
      return 3;
  }
}

/** Root application view — orchestrates form input, listing approval, and confirmation. */
export default function App() {
  const [product, setProduct] = useState('');
  const [price, setPrice] = useState('');
  const [location, setLocation] = useState('');
  const [dateListed, setDateListed] = useState('');
  const [condition, setCondition] = useState('');
  const [color, setColor] = useState('');

  const [loadingState, setLoadingState] = useState<LoadingState>('idle');
  const [selectedListings, setSelectedListings] = useState<number[]>([]);
  /** UI-only — controls optional filters accordion; not sent anywhere special. */
  const [showOptionalFilters, setShowOptionalFilters] = useState(false);
  /** UI-only — fake loader animation; does not control screen transitions. */
  const [loaderProgress, setLoaderProgress] = useState(0);
  const [loaderStep, setLoaderStep] = useState(0);
  /** UI-only — fake candidate count for loader copy; replace with real scan progress from API/SSE. */
  const [loaderListingCount, setLoaderListingCount] = useState(0);

  const fastapiTimeoutRef = useRef<ReturnType<typeof setTimeout> | null>(null);

  const { reset } = useAgentStream({ simulate: true });

  const isFormVisible = loadingState === 'idle';
  const isFastapiLoading = loadingState === 'fastapi_loading';
  const isShowListings = loadingState === 'show_listings';
  const isSuccessConfirmed = loadingState === 'success_confirmed';
  const isLogoExpanded = isFormVisible;

  useEffect(() => {
    return () => {
      if (fastapiTimeoutRef.current) clearTimeout(fastapiTimeoutRef.current);
    };
  }, []);

  /** UI-only — animates loader panel; screen still advances via fastapiTimeoutRef below. */
  useEffect(() => {
    if (loadingState !== 'fastapi_loading') {
      setLoaderProgress(0);
      setLoaderStep(0);
      setLoaderListingCount(0);
      return;
    }

    const start = Date.now();
    const interval = setInterval(() => {
      const elapsed = Date.now() - start;
      const ratio = Math.min(1, elapsed / FASTAPI_LOADING_MS);
      setLoaderProgress(ratio * 100);
      setLoaderStep(Math.min(5, Math.floor(ratio * 6)));
      setLoaderListingCount(Math.min(10, Math.floor(ratio * 12)));
    }, 120);

    return () => clearInterval(interval);
  }, [loadingState]);

  const handleSubmit = (e: FormEvent) => {
    e.preventDefault();
    if (!product.trim() || !price.trim() || !location.trim()) return;

    if (fastapiTimeoutRef.current) clearTimeout(fastapiTimeoutRef.current);

    setSelectedListings([]);
    setLoadingState('fastapi_loading');

    fastapiTimeoutRef.current = setTimeout(() => {
      setLoadingState('show_listings');
      fastapiTimeoutRef.current = null;
    }, FASTAPI_LOADING_MS);

    // TODO: await response, store in listings state, then setLoadingState('show_listings')
    void fetch('http://localhost:8000/api/v1/orchestrate', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        product,
        price: parseFloat(price),
        location,
        dateListed,
        condition,
        color,
      }),
    }).catch((error) => {
      console.error('Failed hitting api.dealbot.ai/v1/orchestrate', error);
    });
  };

  const toggleListingSelection = (listingId: number) => {
    setSelectedListings((prev) => {
      if (prev.includes(listingId)) {
        return prev.filter((id) => id !== listingId);
      }
      if (prev.length >= MAX_LISTING_SELECTIONS) return prev;
      return [...prev, listingId];
    });
  };

  const handleConfirmListings = () => {
    if (selectedListings.length === 0) return;
    setLoadingState('success_confirmed');
  };

  const handleReset = () => {
    if (fastapiTimeoutRef.current) {
      clearTimeout(fastapiTimeoutRef.current);
      fastapiTimeoutRef.current = null;
    }
    reset();
    setLoadingState('idle');
    setSelectedListings([]);
    setProduct('');
    setPrice('');
    setLocation('');
    setDateListed('');
    setCondition('');
    setColor('');
    setShowOptionalFilters(false);
  };

  const hasHuntPreview =
    product.trim() || price.trim() || location.trim();

  return (
    <div className="page-shell">
      <ListingAmbientBackground
        subdued={isShowListings || isSuccessConfirmed}
      />

      <main className="page-main">
        <motion.div
          layout
          animate={{
            scale: isLogoExpanded ? 1 : 0.72,
            y: isLogoExpanded ? 0 : -8,
          }}
          transition={{ type: 'spring', stiffness: 260, damping: 28 }}
          className={`flex flex-col items-center ${isLogoExpanded ? 'logo-wrap-idle' : 'logo-wrap-active'}`}
        >
          <img src={LOGO_SRC} alt="DealBot logo" className="logo-image" />
        </motion.div>

        <JourneyStepper activeIndex={journeyIndex(loadingState)} />

        {/* --- Command embed: anchored query summary during load / listing review --- */}
        <AnimatePresence>
          {(isFastapiLoading || isShowListings) && (
            <motion.div
              key="command-embed"
              initial={{ opacity: 0, y: -6 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -4 }}
              transition={{ duration: 0.3 }}
              className="command-embed-wrap"
            >
              <span className="command-embed-pill" aria-readonly="true">
                {isFastapiLoading && (
                  <span className="command-embed-live" aria-hidden />
                )}
                {product.trim()} · max {price.trim()} · {location.trim()}
                {isFastapiLoading && ' — scanning'}
              </span>
            </motion.div>
          )}
        </AnimatePresence>

        {/* --- STATE 1: Idle — structured search form --- */}
        <AnimatePresence mode="wait">
          {isFormVisible && (
            <motion.form
              key="search-form"
              onSubmit={handleSubmit}
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -20 }}
              transition={{ duration: 0.35 }}
              className="search-form"
            >
              {/* LEFT: pitch */}
              <div className="pitch-panel">
                <div>
                  <p className="pitch-eyebrow">AI-powered deal hunting</p>
                  <h1 className="pitch-headline">
                    You tell us what
                    <br />
                    you want. We <em>get it.</em>
                  </h1>
                </div>
                <p className="pitch-body">
                  Tell us what you want and your budget. We search marketplaces,
                  shortlist matches, and negotiate with sellers for you.
                </p>
                <div className="pitch-section">
                  <p className="pitch-section-label">How it works</p>
                  <p className="pitch-copy">
                    Five agents scan, message, and follow up in parallel. You
                    review the picks and confirm what to pursue.
                  </p>
                </div>
                <div className="pitch-section">
                  <p className="pitch-section-label">Where we look</p>
                  <p className="pitch-copy">
                    Facebook Marketplace, Craigslist, OfferUp, Mercari, and Nextdoor.
                  </p>
                </div>
              </div>

              {/* RIGHT: form fields */}
              <div className="form-panel">
                <div>
                  <h2 className="form-panel-heading">Start your hunt</h2>
                  <p className="form-panel-sub">
                    Fill in what you&apos;re after and we&apos;ll handle the rest.
                  </p>
                </div>

                <div className="bento-form">
                  <div className="bento-panel-product">
                    <label htmlFor="product" className="form-label-required">
                      Product *
                    </label>
                    <input
                      id="product"
                      type="text"
                      required
                      value={product}
                      onChange={(e) => setProduct(e.target.value)}
                      placeholder="e.g. Couch, iPhone 14 Pro, Guitar"
                      className="form-input-product"
                    />
                    <div className="product-chips">
                      {PRODUCT_CHIPS.map((chip) => (
                        <button
                          key={chip}
                          type="button"
                          className={`product-chip ${product === chip ? 'product-chip--active' : ''}`}
                          onClick={() => setProduct(chip)}
                        >
                          {chip}
                        </button>
                      ))}
                    </div>
                  </div>

                  <div className="bento-row-2">
                    <div className="bento-panel-price">
                      <label htmlFor="price" className="form-label-required">
                        Max budget *
                      </label>
                      <input
                        id="price"
                        type="text"
                        required
                        value={price}
                        onChange={(e) => setPrice(e.target.value)}
                        placeholder="e.g. 600"
                        className="form-input-required"
                      />
                      <div className="product-chips product-chips--compact">
                        {BUDGET_CHIPS.map((chip) => (
                          <button
                            key={chip}
                            type="button"
                            className={`product-chip ${price === chip ? 'product-chip--active' : ''}`}
                            onClick={() => setPrice(chip)}
                          >
                            {chip}
                          </button>
                        ))}
                      </div>
                    </div>
                    <div className="bento-panel-location">
                      <label htmlFor="location" className="form-label-required">
                        Location *
                      </label>
                      <input
                        id="location"
                        type="text"
                        required
                        value={location}
                        onChange={(e) => setLocation(e.target.value)}
                        placeholder="e.g. Los Angeles, CA"
                        className="form-input-required"
                      />
                    </div>
                  </div>

                  <button
                    type="button"
                    className="optional-filters-toggle"
                    onClick={() => setShowOptionalFilters((v) => !v)}
                    aria-expanded={showOptionalFilters}
                  >
                    {showOptionalFilters ? '− Hide optional filters' : '+ Add filters (optional)'}
                  </button>

                  {showOptionalFilters && (
                    <div className="bento-row-3 bento-row-3--optional">
                      <div className="bento-panel-date">
                      <div className="field-label-row">
                        <label htmlFor="dateListed" className="form-label-optional">
                          Date listed
                        </label>
                        <span className="form-label-hint">Optional</span>
                      </div>
                      <input
                        id="dateListed"
                        type="text"
                        value={dateListed}
                        onChange={(e) => setDateListed(e.target.value)}
                        placeholder="e.g. Past week"
                        className="form-input-optional"
                      />
                    </div>
                    <div className="bento-panel-condition">
                      <div className="field-label-row">
                        <label htmlFor="condition" className="form-label-optional">
                          Condition
                        </label>
                        <span className="form-label-hint">Optional</span>
                      </div>
                      <input
                        id="condition"
                        type="text"
                        value={condition}
                        onChange={(e) => setCondition(e.target.value)}
                        placeholder="e.g. Like new"
                        className="form-input-optional"
                      />
                    </div>
                    <div className="bento-panel-color">
                      <div className="field-label-row">
                        <label htmlFor="color" className="form-label-optional">
                          Color
                        </label>
                        <span className="form-label-hint">Optional</span>
                      </div>
                      <input
                        id="color"
                        type="text"
                        value={color}
                        onChange={(e) => setColor(e.target.value)}
                        placeholder="e.g. Black"
                        className="form-input-optional"
                      />
                    </div>
                  </div>
                  )}
                </div>

                {hasHuntPreview && (
                  <div className="hunt-preview">
                    <span className="hunt-preview-label">Your hunt</span>
                    <p className="hunt-preview-text">
                      {product.trim() || '…'} · max {price.trim() || '…'} ·{' '}
                      {location.trim() || '…'}
                    </p>
                  </div>
                )}

                <button
                  type="submit"
                  disabled={!product.trim() || !price.trim() || !location.trim()}
                  className="btn-submit"
                >
                  Find me the best deal →
                </button>
                <p className="form-panel-footnote">
                  Your agent will search, contact sellers, and negotiate — results
                  sent to your inbox.
                </p>
              </div>
            </motion.form>
          )}
        </AnimatePresence>

        {/* --- STATE 1b: FastAPI loading --- */}
        <AnimatePresence mode="wait">
          {isFastapiLoading && (
            <motion.div
              key="fastapi-loading"
              initial={{ opacity: 0, y: 16 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -12 }}
              transition={{ duration: 0.35 }}
              className="fastapi-loading"
              aria-live="polite"
              aria-busy="true"
            >
              <LoadingProgress
                progress={loaderProgress}
                activeStep={loaderStep}
                listingCount={loaderListingCount}
              />
            </motion.div>
          )}
        </AnimatePresence>

        {/* --- STATE 1c: Listing approval grid --- */}
        <AnimatePresence mode="wait">
          {isShowListings && (
            <motion.section
              key="listings-grid"
              initial={{ opacity: 0, y: 24 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 16 }}
              transition={{ type: 'spring', stiffness: 220, damping: 26 }}
              className="listings-section"
            >
              <header className="listings-header">
                <h2 className="listings-header-title">Pick up to 5 listings</h2>
                <p className="listings-header-sub">
                  We&apos;ll reach out, negotiate, and follow up on the ones you
                  select.
                </p>
              </header>

              <div className="listings-bento">
                {MOCK_LISTINGS.map((listing) => {
                  const isSelected = selectedListings.includes(listing.id);
                  const isMaxReached =
                    selectedListings.length >= MAX_LISTING_SELECTIONS;
                  const isDisabled = !isSelected && isMaxReached;

                  return (
                    <label
                      key={listing.id}
                      htmlFor={`listing-${listing.id}`}
                      className={`listing-tile ${isSelected ? 'listing-tile--active' : ''} ${isDisabled ? 'listing-tile--disabled' : ''}`}
                    >
                      <div className="listing-image-wrap">
                        <div
                          className="listing-image-placeholder"
                          aria-hidden
                        >
                          <span className="listing-image-icon" aria-hidden>
                            📦
                          </span>
                        </div>
                        <div className="listing-meta-pills">
                          <span className="listing-meta-pill">{listing.source}</span>
                        </div>
                        {isSelected && (
                          <span className="listing-queued-badge">Queued</span>
                        )}
                      </div>
                      <div className="listing-card-body">
                        <p className="listing-card-title">{listing.title}</p>
                        <p className="listing-card-price">{listing.price}</p>
                        <p className="listing-card-area">{listing.area} area</p>
                      </div>
                      <div className="listing-select-row">
                        <input
                          id={`listing-${listing.id}`}
                          type="checkbox"
                          checked={isSelected}
                          disabled={isDisabled}
                          onChange={() => toggleListingSelection(listing.id)}
                          className="sr-only"
                        />
                        <span
                          className={`listing-dot ${isSelected ? 'listing-dot--active' : ''}`}
                          aria-hidden
                        />
                        <span className="listing-select-label">
                          {isSelected ? 'Selected' : 'Select listing'}
                        </span>
                      </div>
                    </label>
                  );
                })}
              </div>

              <div className="listings-footer">
                <p className="listing-selection-count">
                  {selectedListings.length} of {MAX_LISTING_SELECTIONS} selected
                </p>

                <div className="listings-actions">
                  <button
                    type="button"
                    onClick={handleConfirmListings}
                    disabled={selectedListings.length === 0}
                    className="btn-confirm-listings listings-actions-primary"
                  >
                    {selectedListings.length === 0
                      ? 'Select listings to continue'
                      : `Confirm ${selectedListings.length} listing${selectedListings.length > 1 ? 's' : ''} →`}
                  </button>
                </div>
              </div>
            </motion.section>
          )}
        </AnimatePresence>

        {/* --- STATE 1d: Thank you confirmation card --- */}
        <AnimatePresence mode="wait">
          {isSuccessConfirmed && (
            <motion.div
              key="success-confirmed"
              initial={{ opacity: 0, y: 24 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: 16 }}
              transition={{ type: 'spring', stiffness: 220, damping: 26 }}
              className="reset-wrap"
            >
              <div className="success-card">
                <div className="success-icon">
                  <svg
                    width="20"
                    height="20"
                    viewBox="0 0 24 24"
                    fill="none"
                    stroke="var(--green-accent-light)"
                    strokeWidth="2.5"
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    aria-hidden
                  >
                    <path d="M5 13l4 4L19 7" />
                  </svg>
                </div>
                <div>
                  <p className="success-title">Hunt is underway</p>
                  <p className="success-body">
                    Your agents are reaching out, negotiating, and following up.
                    Check your inbox for updates.
                  </p>
                </div>

                <ol className="success-timeline">
                  {/* HARDCODED UI — drive from real hunt status / webhook updates when available. */}
                  <li className="success-timeline-item success-timeline-item--complete">
                    <span className="success-timeline-dot" aria-hidden />
                    Listings approved
                  </li>
                  <li className="success-timeline-item success-timeline-item--active">
                    <span className="success-timeline-dot" aria-hidden />
                    Agents reaching out
                  </li>
                  <li className="success-timeline-item">
                    <span className="success-timeline-dot" aria-hidden />
                    Negotiations
                  </li>
                  <li className="success-timeline-item">
                    <span className="success-timeline-dot" aria-hidden />
                    Best deal sent to inbox
                  </li>
                </ol>

                <div className="success-agents-row">
                  {/* HARDCODED UI — replace with live agent heartbeat from backend. */}
                  {Array.from({ length: 5 }, (_, i) => (
                    <span key={i} className="success-agent-dot" aria-hidden />
                  ))}
                  <span className="success-agents-label">5 agents active</span>
                </div>

                <button type="button" onClick={handleReset} className="btn-reset">
                  Start a new hunt
                </button>
              </div>
            </motion.div>
          )}
        </AnimatePresence>
      </main>
    </div>
  );
}
