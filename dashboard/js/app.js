/* ============================================================
   PEI National Park FWI Dashboard — Application
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
    let dates = [];
    let markers = {};
    let currentDateIndex;

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

    // --- Date Slider Control ---
    function addDateSlider() {
        const slider = L.control({ position: 'bottomleft' });
        slider.onAdd = function () {
            const div = L.DomUtil.create('div', 'date-slider-control');

            const label = document.createElement('span');
            label.className = 'date-label';
            label.id = 'slider-date-label';

            const input = document.createElement('input');
            input.type = 'range';
            input.id = 'date-slider';
            input.min = 0;
            input.max = dates.length - 1;
            input.value = dates.length - 1;

            const btn = document.createElement('button');
            btn.className = 'btn-today';
            btn.textContent = '⏩ Today';

            div.appendChild(label);
            div.appendChild(input);
            div.appendChild(btn);

            // Prevent map drag when interacting with slider
            L.DomEvent.disableClickPropagation(div);
            L.DomEvent.disableScrollPropagation(div);

            input.addEventListener('input', function () {
                currentDateIndex = parseInt(this.value, 10);
                updateMarkers();
            });

            btn.addEventListener('click', function () {
                currentDateIndex = dates.length - 1;
                input.value = currentDateIndex;
                updateMarkers();
            });

            return div;
        };
        slider.addTo(map);
    }

    // --- Station Markers ---
    function createMarkers() {
        for (const [stationId, meta] of Object.entries(stationsData)) {
            const marker = L.circleMarker([meta.lat, meta.lon], {
                radius: 12,
                fillColor: '#bdc3c7',
                color: '#2c3e50',
                weight: 2,
                fillOpacity: 0.85,
            }).addTo(map);

            marker.stationId = stationId;
            marker.bindTooltip(meta.display_name, { direction: 'top', offset: [0, -12] });
            marker.on('click', function () { showPopup(stationId); });

            markers[stationId] = marker;
        }
    }

    // --- Update Markers for Current Date ---
    function updateMarkers() {
        const date = dates[currentDateIndex];
        const records = fwiDaily[date] || [];

        // Update slider label
        const label = document.getElementById('slider-date-label');
        if (label) label.textContent = date;

        // Build lookup for this date
        const byStation = {};
        for (const rec of records) {
            byStation[rec.station] = rec;
        }

        for (const [stationId, marker] of Object.entries(markers)) {
            const rec = byStation[stationId];
            const cls = rec ? fwiClass(rec.fwi) : { label: 'N/A', color: '#bdc3c7' };

            marker.setStyle({ fillColor: cls.color });
            if (!rec) marker.getElement()?.classList.add('marker-no-data');
            else marker.getElement()?.classList.remove('marker-no-data');

            // Update tooltip with FWI class
            const displayName = stationsData[stationId].display_name;
            const tipText = rec
                ? `${displayName} — FWI ${rec.fwi.toFixed(1)} (${cls.label})`
                : `${displayName} — No data`;
            marker.setTooltipContent(tipText);
        }
    }

    // --- Popup with FWI Breakdown + Sparkline ---
    function showPopup(stationId) {
        const marker = markers[stationId];
        const date = dates[currentDateIndex];
        const meta = stationsData[stationId];
        const records = fwiDaily[date] || [];
        const rec = records.find(r => r.station === stationId);

        let html = `<div class="fwi-popup">`;
        html += `<div class="popup-header">📍 ${meta.display_name}</div>`;
        html += `<div class="popup-date">${date}</div>`;

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

        // Sparkline — last 30 days of FWI
        const sparkSvg = buildSparkline(stationId, currentDateIndex, 30);
        if (sparkSvg) {
            html += `<div class="sparkline-container">`;
            html += `<div class="sparkline-label">FWI — Last 30 days</div>`;
            html += sparkSvg;
            html += `</div>`;
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
            const date = dates[i];
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

        // Color the sparkline by the latest FWI class
        const lastFwi = points[points.length - 1].fwi;
        const cls = fwiClass(lastFwi);

        return `<svg class="sparkline" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
            <polyline fill="none" stroke="${cls.color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"
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
        const [stationsResp, fwiResp] = await Promise.all([
            fetch('data/stations.json'),
            fetch('data/fwi_daily.json'),
        ]);

        stationsData = await stationsResp.json();
        fwiDaily = await fwiResp.json();

        // Build sorted date array
        dates = Object.keys(fwiDaily).sort();
        currentDateIndex = dates.length - 1;

        // Initialize map components
        initMap();
        addLegend();
        createMarkers();
        addDateSlider();
        addParkBoundary();

        // Set initial state
        updateMarkers();
    }

    // Boot
    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', main);
    } else {
        main();
    }

})();
