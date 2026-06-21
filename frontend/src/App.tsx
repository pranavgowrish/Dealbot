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
const API_BASE = 'http://localhost:8000';
/** Business rule — max picks before confirm. Change if product allows more/fewer. */
const MAX_LISTING_SELECTIONS = 5;

/** UI-only — quick-fill chips for the search form. */
const PRODUCT_CHIPS = ['Couch', 'iPhone 14 Pro', 'Electric guitar', 'Office chair'];
const BUDGET_CHIPS = ['$300', '$600', '$1000', '$2000'];

type LoadingState =
  | 'idle'
  | 'fastapi_loading'
  | 'show_listings'
  | 'success_confirmed';

type Listing = {
  id: number;
  title: string;
  price: string;
  image_url?: string | null;
  listing_url: string;
  area: string;
  source: string;
};

function parsePrice(value: string): number {
  const parsed = parseFloat(value.replace(/[$,]/g, '').trim());
  return Number.isFinite(parsed) ? parsed : 0;
}

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
  const [listings, setListings] = useState<Listing[]>([]);
  const [selectedListings, setSelectedListings] = useState<number[]>([]);
  const [searchError, setSearchError] = useState<string | null>(null);
  const [orchestrateError, setOrchestrateError] = useState<string | null>(null);
  const [isConfirming, setIsConfirming] = useState(false);
  /** UI-only — controls optional filters accordion; not sent anywhere special. */
  const [showOptionalFilters, setShowOptionalFilters] = useState(false);
  /** UI-only — loader animation driven by elapsed time during search. */
  const [loaderProgress, setLoaderProgress] = useState(0);
  const [loaderStep, setLoaderStep] = useState(0);
  const [loaderListingCount, setLoaderListingCount] = useState(0);

  const loaderIntervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  const { reset } = useAgentStream({ simulate: true });

  const isFormVisible = loadingState === 'idle';
  const isFastapiLoading = loadingState === 'fastapi_loading';
  const isShowListings = loadingState === 'show_listings';
  const isSuccessConfirmed = loadingState === 'success_confirmed';
  const isLogoExpanded = isFormVisible;

  useEffect(() => {
    return () => {
      if (loaderIntervalRef.current) clearInterval(loaderIntervalRef.current);
    };
  }, []);

  useEffect(() => {
    if (loadingState !== 'fastapi_loading') {
      setLoaderProgress(0);
      setLoaderStep(0);
      setLoaderListingCount(0);
      if (loaderIntervalRef.current) {
        clearInterval(loaderIntervalRef.current);
        loaderIntervalRef.current = null;
      }
      return;
    }

    const start = Date.now();
    loaderIntervalRef.current = setInterval(() => {
      const elapsed = Date.now() - start;
      const ratio = Math.min(1, elapsed / 120_000);
      setLoaderProgress(ratio * 95);
      setLoaderStep(Math.min(5, Math.floor(ratio * 6)));
      setLoaderListingCount(Math.min(10, Math.floor(ratio * 12)));
    }, 120);

    return () => {
      if (loaderIntervalRef.current) {
        clearInterval(loaderIntervalRef.current);
        loaderIntervalRef.current = null;
      }
    };
  }, [loadingState]);

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    const parsedPrice = parsePrice(price);
    if (!product.trim() || !location.trim() || parsedPrice <= 0) return;

    setSearchError(null);
    setOrchestrateError(null);
    setSelectedListings([]);
    setListings([]);
    setLoadingState('fastapi_loading');

    try {
      const response = await fetch(`${API_BASE}/api/v1/search`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          product: product.trim(),
          price: parsedPrice,
          location: location.trim(),
          dateListed: dateListed.trim() || undefined,
          condition: condition.trim() || undefined,
          color: color.trim() || undefined,
        }),
      });

      if (!response.ok) {
        const body = await response.json().catch(() => null);
        const detail =
          body && typeof body.detail === 'string'
            ? body.detail
            : `Search failed (${response.status})`;
        throw new Error(detail);
      }

      const data = (await response.json()) as { listings: Listing[] };
      setLoaderProgress(100);
      setLoaderListingCount(data.listings.length);
      setListings(data.listings);

      if (data.listings.length === 0) {
        setSearchError('No listings matched your search. Try adjusting your budget or filters.');
        setLoadingState('idle');
        return;
      }

      setLoadingState('show_listings');
    } catch (error) {
      console.error('Marketplace search failed', error);
      setSearchError(
        error instanceof Error ? error.message : 'Failed to search marketplaces.',
      );
      setLoadingState('idle');
    }
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

  const handleConfirmListings = async () => {
    if (selectedListings.length === 0 || isConfirming) return;

    const parsedPrice = parsePrice(price);
    const selectedUrls = selectedListings
      .map((id) => listings.find((listing) => listing.id === id)?.listing_url)
      .filter((url): url is string => Boolean(url));

    if (selectedUrls.length === 0) return;

    setIsConfirming(true);
    setOrchestrateError(null);

    try {
      const response = await fetch(`${API_BASE}/api/v1/orchestrate`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          product: product.trim(),
          budget: parsedPrice,
          max_price: parsedPrice,
          listing_urls: selectedUrls,
        }),
      });

      if (!response.ok) {
        const body = await response.json().catch(() => null);
        const detail =
          body && typeof body.detail === 'string'
            ? body.detail
            : `Orchestration failed (${response.status})`;
        throw new Error(detail);
      }

      setLoadingState('success_confirmed');
    } catch (error) {
      console.error('Orchestration failed', error);
      setOrchestrateError(
        error instanceof Error
          ? error.message
          : 'Failed to start agent orchestration.',
      );
    } finally {
      setIsConfirming(false);
    }
  };

  const handleReset = () => {
    reset();
    setLoadingState('idle');
    setListings([]);
    setSelectedListings([]);
    setSearchError(null);
    setOrchestrateError(null);
    setIsConfirming(false);
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

                {searchError && (
                  <p className="form-panel-sub" role="alert">
                    {searchError}
                  </p>
                )}

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
                {listings.map((listing) => {
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
                        {listing.image_url ? (
                          <img
                            src={listing.image_url}
                            alt=""
                            className="listing-image-placeholder"
                          />
                        ) : (
                          <div
                            className="listing-image-placeholder"
                            aria-hidden
                          >
                            <span className="listing-image-icon" aria-hidden>
                              📦
                            </span>
                          </div>
                        )}
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

                {orchestrateError && (
                  <p className="listing-selection-count" role="alert">
                    {orchestrateError}
                  </p>
                )}

                <div className="listings-actions">
                  <button
                    type="button"
                    onClick={handleConfirmListings}
                    disabled={selectedListings.length === 0 || isConfirming}
                    className="btn-confirm-listings listings-actions-primary"
                  >
                    {isConfirming
                      ? 'Starting agents…'
                      : selectedListings.length === 0
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
                  {Array.from({ length: selectedListings.length }, (_, i) => (
                    <span key={i} className="success-agent-dot" aria-hidden />
                  ))}
                  <span className="success-agents-label">
                    {selectedListings.length} agent{selectedListings.length > 1 ? 's' : ''} active
                  </span>
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
