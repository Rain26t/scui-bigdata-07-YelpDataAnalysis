"""Custom CSS styles for the Yelp Text-to-SQL UI."""


def get_custom_css(bg_b64: str = "", bg_mime: str = "image/jpeg") -> str:
    """Return the full CSS string, optionally embedding a background image as base64."""
    bg_image_css = (
        f"url('data:{bg_mime};base64,{bg_b64}')"
        if bg_b64
        else "none"
    )
    return _CSS_TEMPLATE.replace("__BG_IMAGE__", bg_image_css)


_CSS_TEMPLATE = """
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
@import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&display=swap');

:root {
    --color-bg: #0a0a0a;
    --color-surface: #141414;
    --color-surface-2: #1e1e1e;
    --color-border: rgba(255, 255, 255, 0.08);
    --color-border-hover: rgba(203, 170, 116, 0.4);
    --color-text-primary: #f5f5f5;
    --color-text-secondary: rgba(245, 245, 245, 0.55);
    --color-bronze: #cbaa74;
    --color-bronze-light: #e2c99a;
    --color-bronze-glow: rgba(203, 170, 116, 0.15);
    --font-sans: 'Inter', -apple-system, sans-serif;
    --font-mono: 'JetBrains Mono', monospace;
    --radius-sm: 8px;
    --radius-md: 14px;
    --radius-lg: 22px;
    --radius-pill: 999px;
    --shadow-card: 0 4px 24px rgba(0,0,0,0.4);
    --shadow-glow: 0 0 40px rgba(203, 170, 116, 0.08);
    --transition-fast: 0.18s cubic-bezier(0.22, 1, 0.36, 1);
    --transition-smooth: 0.35s cubic-bezier(0.22, 1, 0.36, 1);
}

/* --- Scroll & Reveal Animation --- */
@keyframes reveal {
    from {
        opacity: 0;
        transform: translateY(10px);
    }
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.revealable {
    animation: reveal 0.8s cubic-bezier(0.22, 1, 0.36, 1) forwards;
    animation-delay: 0.1s;
    opacity: 0;
}


/* --- Global Reset & Streamlit Chrome Removal --- */

body {
    background-color: var(--color-bg);
    font-family: var(--font-sans);
    color: var(--color-text-primary);
    line-height: 1.6;
}

/* Hide Streamlit's default chrome */
#stDecoration,
#MainMenu,
.stDeployButton,
footer {
    display: none !important;
    visibility: hidden !important;
}

/* Main app container styling */
[data-testid="stAppViewContainer"] {
    background-image: linear-gradient(to bottom, rgba(10, 10, 10, 0.35), rgba(10, 10, 10, 0.95) 65%), __BG_IMAGE__;
    background-size: contain;
    background-position: center center;
    background-repeat: no-repeat;
    background-attachment: fixed;
    background-color: #080610;
    padding: 0 !important;
}

/* --- Glassmorphic Card --- */
.glass-card {
    background-color: rgba(20, 20, 20, 0.6);
    backdrop-filter: blur(12px);
    -webkit-backdrop-filter: blur(12px);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-lg);
    padding: 1.5rem 2rem;
    margin-bottom: 1.5rem;
    box-shadow: 0 0 40px var(--color-bronze-glow);
}

/* --- Component Classes --- */

/* A. Hyperlink Navigation Pill */
.nav-pill {
    display: inline-flex;
    align-items: center;
    padding: 0.42rem 1rem;
    font-size: 0.78rem;
    font-weight: 600;
    color: var(--color-text-secondary);
    background-color: transparent;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-pill);
    text-decoration: none;
    transition: all var(--transition-fast);
}

.nav-pill:hover {
    color: var(--color-bronze);
    border-color: var(--color-border-hover);
    background-color: var(--color-bronze-glow);
    text-decoration: none;
}

/* B. Feature Hyperlink Card */
.feature-link-card {
    display: flex;
    align-items: center;
    gap: 1.2rem;
    width: 100%;
    padding: 1.1rem 1.4rem;
    background-color: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    text-decoration: none;
    transform: translateY(0);
    box-shadow: none;
    transition: all var(--transition-smooth);
}

.feature-link-card:hover {
    border-color: var(--color-border-hover);
    transform: translateY(-2px);
    box-shadow: var(--shadow-card);
    text-decoration: none;
}

/* C. Section Divider */
.section-divider {
    margin: 2.5rem 0;
    border: none;
    border-top: 1px solid var(--color-border);
    text-align: center;
    overflow: visible;
}

.section-divider::before {
    content: attr(data-label);
    position: relative;
    top: -0.7em;
    padding: 0 1em;
    background-color: var(--color-bg);
    color: var(--color-text-secondary);
    font-size: 0.8rem;
    font-weight: 500;
}

/* D. Keyword Hyperlink */
.keyword-link {
    position: relative;
    display: inline;
    color: var(--color-bronze);
    border-bottom: none;
    text-decoration: none;
    padding: 0.1em 0.2em;
    transition: all var(--transition-fast);
}

.keyword-link::after {
    content: '';
    position: absolute;
    width: 0;
    height: 1px;
    display: block;
    margin-top: 2px;
    right: 0;
    background: var(--color-bronze);
    transition: width .3s ease;
    -webkit-transition: width .3s ease;
}

.keyword-link:hover::after {
    width: 100%;
    left: 0;
    background-color: var(--color-bronze);
}

.keyword-link:hover {
    color: var(--color-bronze-light);
    background-color: var(--color-bronze-glow);
    border-color: var(--color-bronze);
    text-decoration: none;
}

/* E. Section Title */
.section-title {
    font-size: 0.7rem;
    font-weight: 700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: var(--color-text-secondary);
    margin-bottom: 1rem;
}

/* F. Hero Stat Card */
.stat-card {
    background-color: var(--color-surface);
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    padding: 1.2rem 1.5rem;
    transition: all var(--transition-smooth);
    box-shadow: none;
}

.stat-card:hover {
    border-color: var(--color-border-hover);
    box-shadow: var(--shadow-glow);
}

.stat-card .stat-number {
    font-size: 2rem;
    font-weight: 700;
    color: var(--color-text-primary);
    line-height: 1.2;
}

.stat-card .stat-subtitle {
    font-size: 0.9rem;
    color: var(--color-text-secondary);
    font-family: var(--font-mono);
}

/* --- ChatGPT Chat UI Overrides --- */

/* 1. The Chat Container (The Canvas) */
/* This targets the container that holds all the chat messages */
[data-testid="stChatMessages"] {
    margin: 0 auto;
    max-width: 800px;
    padding-bottom: 8rem; /* Space for the fixed input */
    background-color: var(--color-surface); /* Smooth integration with dark theme */
    border: none;
    box-shadow: none;
}

/* 2. General Message Styling */
.stChatMessage {
    border: none !important;
    box-shadow: none !important;
    background-color: transparent !important;
    padding-left: 2rem;
    padding-right: 2rem;
}

/* 3. User Messages (Right-aligned / ChatGPT Style) */
div[data-testid="stChatMessage"]:has(div[data-testid="stAvatarIcon-user"]) {
    display: flex;
    justify-content: flex-end;
}

div[data-testid="stChatMessage"]:has(div[data-testid="stAvatarIcon-user"]) .stChatMessageContent {
    background-color: #2f2f2f;
    color: #ececec;
    border-radius: 18px;
    padding: 0.8rem 1.1rem;
    max-width: 85%;
}

/* Hide default Streamlit user avatar */
div[data-testid="stAvatarIcon-user"] {
    display: none;
}

/* 4. Assistant Messages (Left-aligned / Miel) */
div[data-testid="stChatMessage"]:has(div[data-testid="stAvatarIcon-assistant"]) {
    justify-content: flex-start;
}

div[data-testid="stChatMessage"]:has(div[data-testid="stAvatarIcon-assistant"]) .stChatMessageContent {
    background-color: transparent;
    color: var(--color-text-primary);
    font-family: var(--font-sans);
    font-size: 16px;
    line-height: 1.6;
    padding: 0.5rem 0;
}

/* Custom Assistant Avatar */
div[data-testid="stAvatarIcon-assistant"] > div {
    background-color: var(--color-bronze);
    color: var(--color-bg);
    font-weight: 700;
    font-size: 1rem;
    width: 40px;
    height: 40px;
    display: flex;
    align-items: center;
    justify-content: center;
}

div[data-testid="stAvatarIcon-assistant"] > div::after {
    content: 'M'; /* M for Miel */
}

/* 5. The Chat Input Box (Fixed Bottom) */
[data-testid="stChatInput"] {
    position: fixed;
    bottom: 1rem;
    left: 50%;
    transform: translateX(-50%);
    width: 100%;
    max-width: 800px; /* Match chat container width */
    background-color: #212121;
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 24px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.3);
    padding: 0.5rem 1.2rem;
    z-index: 100;
}

/* 6. Markdown & Code Block Polish */
.stChatMessage pre {
    background-color: #0d0d0d;
    border-radius: var(--radius-md);
    border: 1px solid var(--color-border);
    position: relative;
    padding-top: 2.5rem; /* Space for the header */
}

.stChatMessage pre::before {
    content: 'SQL Query'; /* Header text */
    position: absolute;
    top: 0;
    left: 0;
    right: 0;
    padding: 0.5rem 1rem;
    background-color: #1e1e1e;
    color: var(--color-text-secondary);
    font-size: 0.8rem;
    font-weight: 600;
    border-bottom: 1px solid var(--color-border);
    border-top-left-radius: var(--radius-md);
    border-top-right-radius: var(--radius-md);
}

.stChatMessage pre::after {
    content: 'Copy'; /* "Copy button" visual */
    position: absolute;
    top: 0.5rem;
    right: 1rem;
    font-size: 0.8rem;
    color: var(--color-text-secondary);
    cursor: pointer;
    opacity: 0.6;
    transition: opacity var(--transition-fast);
}

.stChatMessage pre:hover::after {
    opacity: 1;
}

.stChatMessage code {
    font-size: 0.9rem;
    color: #f0f0f0;
    font-family: var(--font-mono);
}

/* --- Minimalist Tables --- */
.stChatMessage table, .results-table table {
    width: 100%;
    border-collapse: collapse;
    font-family: var(--font-sans);
}

.stChatMessage th, .results-table th {
    background-color: transparent;
    color: var(--color-bronze);
    font-weight: 600;
    border: none;
    border-bottom: 1px solid var(--color-bronze);
    padding: 0.8rem 1rem;
    text-align: left;
}

.stChatMessage td, .results-table td {
    border: none;
    border-bottom: 1px solid var(--color-border);
    padding: 0.8rem 1rem;
    color: var(--color-text-secondary);
}

.stChatMessage tr:nth-child(even) td, .results-table tr:nth-child(even) td {
    background-color: #1a1a1a;
}

.stChatMessage tr:hover td, .results-table tr:hover td {
    background-color: var(--color-bronze-glow);
    color: var(--color-text-primary);
}

/* --- Empty State & Success Toast --- */
.empty-state {
    text-align: center;
    padding: 4rem 2rem;
    background-color: var(--color-surface);
    border-radius: var(--radius-lg);
    border: 1px dashed var(--color-border);
}

.empty-state-icon {
    font-size: 3rem;
    margin-bottom: 1rem;
}

.success-toast {
    position: fixed;
    top: 2rem;
    right: 2rem;
    background-color: #2f2f2f;
    color: #ececec;
    padding: 1rem 1.5rem;
    border-radius: var(--radius-md);
    border: 1px solid var(--color-bronze);
    box-shadow: 0 4px 20px rgba(0,0,0,0.5);
    z-index: 9999;
    display: flex;
    align-items: center;
    gap: 1rem;
    opacity: 0;
    transform: translateY(-20px);
    animation: toast-in 0.5s forwards;
}

@keyframes toast-in {
    to {
        opacity: 1;
        transform: translateY(0);
    }
}

.stChatMessage table {
    width: 100%;
    border-collapse: collapse;
}

.stChatMessage th {
    background-color: #1e1e1e;
    color: var(--color-text-primary);
    font-weight: 600;
    border: 1px solid var(--color-border);
    padding: 0.6rem 0.8rem;
}

.stChatMessage td {
    border: 1px solid var(--color-border);
    padding: 0.6rem 0.8rem;
    color: var(--color-text-secondary);
}

/* --- UX Final Polish & Rich Components --- */

/* Keyboard Shortcut Tooltip */
.shortcut-guide {
    position: absolute;
    bottom: 100%;
    left: 50%;
    transform: translateX(-50%);
    margin-bottom: 10px;
    background-color: #2f2f2f;
    color: #ececec;
    padding: 0.5rem 1rem;
    border-radius: var(--radius-md);
    font-size: 0.8rem;
    white-space: nowrap;
    opacity: 0;
    visibility: hidden;
    transition: all var(--transition-fast);
}

[data-testid="stChatInput"]:hover .shortcut-guide {
    opacity: 1;
    visibility: visible;
}

/* Recent Insights (Query History) */
.recent-insights {
    padding: 1rem;
}

.recent-insights h3 {
    font-size: 0.8rem;
    font-weight: 600;
    color: var(--color-text-secondary);
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-bottom: 1rem;
}

.recent-insights ul {
    list-style: none;
    padding: 0;
    margin: 0;
}

.recent-insights li a {
    display: block;
    padding: 0.6rem 1rem;
    font-size: 0.9rem;
    color: var(--color-text-secondary);
    text-decoration: none;
    border-radius: var(--radius-sm);
    transition: all var(--transition-fast);
}

.recent-insights li a:hover {
    background-color: var(--color-surface-2);
    color: var(--color-text-primary);
}

/* Copy to Clipboard Links */
.code-actions {
    display: flex;
    gap: 1rem;
    justify-content: flex-end;
    padding: 0.5rem 0;
}

.code-actions a {
    font-size: 0.8rem;
    color: var(--color-text-secondary);
    text-decoration: none;
    transition: color var(--transition-fast);
}

.code-actions a:hover {
    color: var(--color-bronze);
}

/* --- Intelligence Mesh & Recommendations --- */
.recommendation-mesh {
    margin-top: 2rem;
    padding-top: 2rem;
    border-top: 1px solid var(--color-border);
}

.recommendation-header {
    font-size: 0.8rem;
    color: var(--color-bronze);
    margin-bottom: 1rem;
    font-family: var(--font-mono);
    text-transform: uppercase;
    letter-spacing: 0.05em;
}

.recommendation-grid {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 1rem;
}

.insight-card {
    background-color: transparent;
    border: 1px solid var(--color-border);
    border-radius: var(--radius-md);
    padding: 1.5rem 1rem;
    text-align: center;
    transition: all var(--transition-smooth);
    animation: reveal 0.5s forwards;
    opacity: 0; /* Start hidden for animation */
}

.insight-card:hover {
    border-color: var(--color-bronze);
    background-color: var(--color-bronze-glow);
    transform: translateY(-4px);
    box-shadow: var(--shadow-glow);
}

.insight-card .icon {
    font-size: 1.5rem;
    color: var(--color-bronze);
    margin-bottom: 0.5rem;
}

.insight-card a {
    text-decoration: none;
    color: var(--color-text-primary);
    font-weight: 500;
    font-size: 0.9rem;
}

/* --- Control Center Footer --- */
.control-center {
    margin-top: 5rem;
    padding: 3rem 2rem;
    background-color: rgba(20, 20, 20, 0.7);
    backdrop-filter: blur(10px);
    border-top: 1px solid var(--color-border);
    border-radius: var(--radius-lg) var(--radius-lg) 0 0;
}

.control-center h2 {
    font-size: 1.5rem;
    font-weight: 600;
    color: var(--color-text-primary);
    margin-bottom: 2rem;
}

.control-grid {
    display: grid;
    grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
    gap: 2rem;
}

.control-section h3 {
    font-size: 1rem;
    font-weight: 600;
    color: var(--color-bronze);
    margin-bottom: 1rem;
}

.control-section ul {
    list-style: none;
    padding: 0;
}

.control-section li a {
    color: var(--color-text-secondary);
    text-decoration: none;
    display: block;
    padding: 0.5rem 0;
    transition: color var(--transition-fast);
}

.control-section li a:hover {
    color: var(--color-bronze-light);
}

.footer-bottom {
    border-top: 1px solid var(--color-border);
    margin-top: 2rem;
    padding-top: 1.5rem;
    text-align: center;
    font-size: 0.8rem;
    color: var(--color-text-secondary);
}

.footer-bottom a {
    color: var(--color-text-secondary);
    text-decoration: none;
    transition: color var(--transition-fast);
}

.footer-bottom a:hover {
    color: var(--color-bronze);
}

/* --- Recent Insights History --- */
.history-link {
    display: block;
    padding: 0.6rem 1rem;
    font-size: 0.9rem;
    color: var(--color-text-secondary);
    text-decoration: none;
    border-radius: var(--radius-sm);
    transition: all var(--transition-fast);
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.history-link:hover {
    background-color: var(--color-surface-2);
    color: var(--color-text-primary);
}

.text-muted {
    color: var(--color-text-secondary);
    font-size: 0.9rem;
    padding: 0.6rem 1rem;
}
"""

# Backward-compat alias – use get_custom_css() for background support
APPLY_CUSTOM_CSS = get_custom_css()
