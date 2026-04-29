/* ============================================================
   PEI National Park FWI Dashboard — Application
   Phase 17: Historical + Forecast dual-mode
   ============================================================ */

(function () {
    'use strict';

    // --- FWI Classification ---
    const FWI_CLASSES = [
        { label: 'Low',       min: 0,    max: 5,    color: '#2ecc71' },
        { label: 'Moderate',  min: 5.1,  max: 14,   color: '#f1c40f' },
        { label: 'High',      min: 14.1, max: 24,   color: '#e67e22' },
        { label: 'Very High', min: 24.1, max: 36,   color: '#e74c3c' },
        { label: 'Extreme',   min: 36.1, max: Infinity, color: '#8e44ad' },
    ];

    function fwiClass(fwi) {
        if (fwi == null || isNaN(fwi)) return { label: 'N/A', color: '#bdc3c7' };
        for (const cls of FWI_CLASSES) {
            if (fwi >= cls.min && fwi <= cls.max) return cls;
        }
        return { label: 'Extreme', color: '#8e44ad' };
    }

    // --- Globals ---
    let map;
    let stationsData = {};
    let fwiDaily = {};
    let fwiForecast = {};
    let forecastMeta = null;
    let histDates = [];
    let fcstDates = [];
    let allDates = [];
    let markers = {};
    let currentDateIndex = 0;
    let currentMode = 'historical'; // 'historical' | 'forecast'

    // --- Map Initialization ---
    function initMap() {
        map = L.map('map', {
            center: [46.45, -63.0],
            zoom: 9,
            zoomControl: true,
        });

        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors',
            maxZoom: 17,
        }).addTo(map);
    }

    // --- Legend Control ---
    function addLegend() {
        const legend = L.control({ position: 'topright' });
        legend.onAdd = function () {
            const div = L.DomUtil.create('div', 'fwi-legend');
            let html = '<h4>Fire Weather Index</h4>';
            for (const cls of FWI_CLASSES) {
                const range = cls.max === Infinity ? `${cls.min}+` : `${cls.min} – ${cls.max}`;
                html += `<div class="legend-item">
                    <span class="legend-swatch" style="background:${cls.color}"></span>
                    <span>${cls.label}</span>
                    <span class="legend-range">(${range})</span>
                </div>`;
            }
            div.innerHTML = html;
            return div;
        };
        legend.addTo(map);
    }

    // --- Mode Toggle Control ---
    function addModeToggle() {
        const ctrl = L.control({ position: 'topleft' });
        ctrl.onAdd = function () {
            const div = L.DomUtil.create('div', 'mode-toggle-control');
            div.innerHTML = `
                <button class="mode-btn active" data-mode="historical">📅 Historical</button>
                <button class="mode-btn" data-mode="forecast">🔮 Forecast</button>
            `;
            div.querySelectorAll('.mode-btn').forEach(btn => {
                btn.addEventListener('click', function () {
                    const mode = this.dataset.mode;
                    if (mode === currentMode) return;
                    setMode(mode);
                    div.querySelectorAll('.mode-btn').forEach(b => b.classList.remove('active'));
                    this.classList.add('active');
                });
            });
            L.DomEvent.disableClickPropagation(div);
            return div;
        };
        ctrl.addTo(map);
    }

    function setMode(mode) {
        currentMode = mode;
        rebuildDateArrays();
        currentDateIndex = allDates.length - 1;
        updateMarkers();
        updateSliderRange();
        updateStalenessBanner();
    }

    function rebuildDateArrays() {
        histDates = Object.keys(fwiDaily).sort();
        fcstDates = Object.keys(fwiForecast).sort();

        if (currentMode === 'forecast') {
            allDates = fcstDates;
        } else {
            allDates = histDates;
        }
    }

    // --- Staleness Banner ---
    function updateStalenessBanner() {
        let banner = document.getElementById('staleness-banner');
        if (currentMode !== 'forecast') {
            if (banner) banner.remove();
            return;
        }
        if (!forecastMeta || !forecastMeta.generated_at) {
            if (banner) banner.remove();
            return;
        }
        const generated = new Date(forecastMeta.generated_at);
        const now = new Date();
        const hoursOld = (now - generated) / (1000 * 60 * 60);
        const stale = hoursOld > 12;

        if (!banner) {
            banner = document.createElement('div');
            banner.id = 'staleness-banner';
            document.getElementById('map').insertAdjacentElement('beforebegin', banner);
        }

        if (stale) {
            banner.className = 'staleness-banner stale';
            banner.textContent = `⚠ Forecast data is ${Math.round(hoursOld)}h old (generated ${generated.toISOString().slice(0, 16)} UTC).`;
        } else {
            banner.className = 'staleness-banner fresh';
            banner.textContent = `Forecast generated ${generated.toISOString().slice(0, 16)} UTC.`;
        }
    }

    // --- Date Slider Control ---
    let sliderControl;

    function addDateSlider() {
        sliderControl = L.control({ position: 'bottomleft' });
        sliderControl.onAdd = function () {
            const div = L.DomUtil.create('div', 'date-slider-control');

            const label = document.createElement('span');
            label.className = 'date-label';
            label.id = 'slider-date-label';

            const input = document.createElement('input');
            input.type = 'range';
            input.id = 'date-slider';
            input.min = 0;
            input.max = 0;
            input.value = 0;

            const btn = document.createElement('button');
            btn.className = 'btn-today';
            btn.textContent = '⏩ Latest';

            div.appendChild(label);
            div.appendChild(input);
            div.appendChild(btn);

            L.DomEvent.disableClickPropagation(div);
            L.DomEvent.disableScrollPropagation(div);

            input.addEventListener('input', function () {
                currentDateIndex = parseInt(this.value, 10);
                updateMarkers();
            });

            btn.addEventListener('click', function () {
                currentDateIndex = allDates.length - 1;
                input.value = currentDateIndex;
                updateMarkers();
            });

            return div;
        };
        sliderControl.addTo(map);
    }

    function updateSliderRange() {
        const input = document.getElementById('date-slider');
        if (!input) return;
        input.min = 0;
        input.max = Math.max(0, allDates.length - 1);
        input.value = currentDateIndex;
    }

    // --- Station Markers ---
    function createMarkers() {
        for (const [stationId, meta] of Object.entries(stationsData)) {
            // Handle stations without coordinates (e.g., Red Head)
            let lat = meta.lat;
            let lon = meta.lon;
            let hasCoords = true;
            
            if (lat == null || lon == null) {
                // Place at center of map with indicator
                lat = 46.45;
                lon = -63.0;
                hasCoords = false;
            }
            
            const marker = L.circleMarker([lat, lon], {
                radius: hasCoords ? 12 : 8,
                fillColor: hasCoords ? '#bdc3c7' : '#95a5a6',
                color: hasCoords ? '#2c3e50' : '#7f8c8d',
                weight: 2,
                fillOpacity: hasCoords ? 0.85 : 0.6,
                dashArray: hasCoords ? null : '4 3',
            }).addTo(map);

            marker.stationId = stationId;
            marker.hasCoords = hasCoords;
            marker.bindTooltip(meta.display_name + (hasCoords ? '' : ' (no GPS)'), { direction: 'top', offset: [0, -12] });
            marker.on('click', function () { showPopup(stationId); });

            markers[stationId] = marker;
        }
    }

    // --- Update Markers for Current Date ---
    function updateMarkers() {
        if (allDates.length === 0) return;
        const date = allDates[currentDateIndex];
        const isForecast = currentMode === 'forecast';
        const dataSource = isForecast ? fwiForecast : fwiDaily;
        const records = dataSource[date] || [];

        // Update slider label
        const label = document.getElementById('slider-date-label');
        if (label) {
            const modeTag = isForecast ? ' [FORECAST]' : '';
            label.textContent = `${date}${modeTag}`;
        }

        // Build lookup for this date
        const byStation = {};
        for (const rec of records) {
            byStation[rec.station] = rec;
        }

        for (const [stationId, marker] of Object.entries(markers)) {
            const rec = byStation[stationId];
            const cls = rec ? fwiClass(rec.fwi) : { label: 'N/A', color: '#bdc3c7' };

            const style = {
                fillColor: cls.color,
                color: isForecast ? '#e67e22' : '#2c3e50',
                weight: isForecast ? 3 : 2,
                dashArray: isForecast ? '4 3' : null,
                fillOpacity: 0.85,
            };
            marker.setStyle(style);

            if (!rec) marker.getElement()?.classList.add('marker-no-data');
            else marker.getElement()?.classList.remove('marker-no-data');

            const displayName = stationsData[stationId].display_name;
            const tipText = rec
                ? `${displayName} — FWI ${rec.fwi.toFixed(1)} (${cls.label})${isForecast ? ' [FCST]' : ''}`
                : `${displayName} — No data`;
            marker.setTooltipContent(tipText);
        }
    }

    // --- Popup with FWI Breakdown + Sparkline ---
    function showPopup(stationId) {
        const marker = markers[stationId];
        const date = allDates[currentDateIndex];
        const meta = stationsData[stationId];
        const isForecast = currentMode === 'forecast';
        const dataSource = isForecast ? fwiForecast : fwiDaily;
        const records = dataSource[date] || [];
        const rec = records.find(r => r.station === stationId);

        let html = `<div class="fwi-popup">`;
        html += `<div class="popup-header">📍 ${meta.display_name}</div>`;
        
        // Station metadata
        html += `<div class="station-meta" style="font-size:0.75rem;color:#666;margin:8px 0;">`;
        if (meta.lat && meta.lon) {
            html += `<div>📍 ${meta.lat.toFixed(4)}, ${meta.lon.toFixed(4)}</div>`;
        } else if (meta.notes) {
            html += `<div>📍 Coordinates unavailable</div>`;
        }
        if (meta.date_established) {
            html += `<div>📅 Established: ${meta.date_established}</div>`;
        }
        if (meta.responsibility) {
            html += `<div>🏢 ${meta.responsibility}</div>`;
        }
        if (meta.transmission) {
            html += `<div>📡 ${meta.transmission}</div>`;
        }
        if (meta.notes && meta.notes.trim()) {
            html += `<div style="margin-top:4px;font-style:italic;">${meta.notes}</div>`;
        }
        html += `</div>`;
        
        html += `<div class="popup-date">${date}`;
        if (isForecast) html += ` <span class="fcst-badge">FORECAST</span>`;
        html += `</div>`;

        if (rec) {
            const cls = fwiClass(rec.fwi);
            html += `<table>`;
            html += row('FFMC', rec.ffmc);
            html += row('DMC', rec.dmc);
            html += row('DC', rec.dc);
            html += row('ISI', rec.isi);
            html += row('BUI', rec.bui);
            html += `<tr class="fwi-value-row"><th>FWI</th><td>${fmt(rec.fwi)} <span class="fwi-class-badge" style="background:${cls.color}">${cls.label}</span></td></tr>`;
            html += `</table>`;
        } else {
            html += `<p style="color:#999;font-size:0.85rem;">No data for this date.</p>`;
        }

        // Sparkline
        if (isForecast) {
            const sparkSvg = buildForecastSparkline(stationId, currentDateIndex);
            if (sparkSvg) {
                html += `<div class="sparkline-container">`;
                html += `<div class="sparkline-label">FWI — 10-day Forecast</div>`;
                html += sparkSvg;
                html += `</div>`;
            }
        } else {
            const sparkSvg = buildSparkline(stationId, currentDateIndex, 30);
            if (sparkSvg) {
                html += `<div class="sparkline-container">`;
                html += `<div class="sparkline-label">FWI — Last 30 days</div>`;
                html += sparkSvg;
                html += `</div>`;
            }
        }

        html += `</div>`;

        marker.bindPopup(html, { maxWidth: 280 }).openPopup();
    }

    function row(label, value) {
        return `<tr><th>${label}</th><td>${fmt(value)}</td></tr>`;
    }

    function fmt(v) {
        return (v != null && !isNaN(v)) ? v.toFixed(1) : '—';
    }

    // --- Sparkline (SVG polyline, no external library) ---
    function buildSparkline(stationId, endIndex, lookback) {
        const startIndex = Math.max(0, endIndex - lookback + 1);
        const points = [];

        for (let i = startIndex; i <= endIndex; i++) {
            const date = allDates[i];
            const records = fwiDaily[date] || [];
            const rec = records.find(r => r.station === stationId);
            if (rec && rec.fwi != null && !isNaN(rec.fwi)) {
                points.push({ index: i, fwi: rec.fwi });
            }
        }

        if (points.length < 2) return null;

        const width = 200;
        const height = 40;
        const padX = 2;
        const padY = 4;

        const fwiVals = points.map(p => p.fwi);
        const minFwi = Math.min(...fwiVals);
        const maxFwi = Math.max(...fwiVals);
        const range = maxFwi - minFwi || 1;

        const coords = points.map((p, idx) => {
            const x = padX + (idx / (points.length - 1)) * (width - 2 * padX);
            const y = padY + (1 - (p.fwi - minFwi) / range) * (height - 2 * padY);
            return `${x.toFixed(1)},${y.toFixed(1)}`;
        });

        const lastFwi = points[points.length - 1].fwi;
        const cls = fwiClass(lastFwi);

        return `<svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
            <polyline fill="none" stroke="${cls.color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
                points="${coords.join(' ')}" />
        </svg>`;
    }

    function buildForecastSparkline(stationId, endIndex) {
        const points = [];

        for (let i = 0; i <= endIndex && i < fcstDates.length; i++) {
            const date = fcstDates[i];
            const records = fwiForecast[date] || [];
            const rec = records.find(r => r.station === stationId);
            if (rec && rec.fwi != null && !isNaN(rec.fwi)) {
                points.push({ index: i, fwi: rec.fwi });
            }
        }

        if (points.length < 2) return null;

        const width = 200;
        const height = 40;
        const padX = 2;
        const padY = 4;

        const fwiVals = points.map(p => p.fwi);
        const minFwi = Math.min(...fwiVals);
        const maxFwi = Math.max(...fwiVals);
        const range = maxFwi - minFwi || 1;

        const coords = points.map((p, idx) => {
            const x = padX + (idx / (points.length - 1)) * (width - 2 * padX);
            const y = padY + (1 - (p.fwi - minFwi) / range) * (height - 2 * padY);
            return `${x.toFixed(1)},${y.toFixed(1)}`;
        });

        const lastFwi = points[points.length - 1].fwi;
        const cls = fwiClass(lastFwi);

        return `<svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
            <polyline fill="none" stroke="${cls.color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" stroke-dasharray="4 2"
                points="${coords.join(' ')}" />
        </svg>`;
    }

    // --- Park Boundary ---
    async function addParkBoundary() {
        try {
            const resp = await fetch('data/park_boundary.geojson');
            if (!resp.ok) return;
            const geojson = await resp.json();
            L.geoJSON(geojson, {
                style: {
                    color: '#1a5276',
                    weight: 2,
                    fillColor: '#aed6f1',
                    fillOpacity: 0.15,
                    dashArray: '6 3',
                },
            }).addTo(map);
        } catch (e) {
            console.warn('Park boundary not loaded:', e);
        }
    }

    // --- Main ---
    async function main() {
        // Load data
        const [stationsResp, fwiResp, fcstResp, metaResp] = await Promise.all([
            fetch('data/stations.json'),
            fetch('data/fwi_daily.json'),
            fetch('data/fwi_forecast.json').catch(() => null),
            fetch('data/forecast_meta.json').catch(() => null),
        ]);

        stationsData = await stationsResp.json();
        fwiDaily = await fwiResp.json();

        if (fcstResp && fcstResp.ok) {
            fwiForecast = await fcstResp.json();
        }
        if (metaResp && metaResp.ok) {
            forecastMeta = await metaResp.json();
        }

        // Build date arrays
        rebuildDateArrays();
        currentDateIndex = allDates.length - 1;

        // Initialize map components
        initMap();
        addLegend();
        addModeToggle();
        createMarkers();
        addDateSlider();
        addParkBoundary();

        // Set initial state
        updateSliderRange();
        updateMarkers();
    }

    // Boot
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', main);
    } else {
        main();
    }

})();
