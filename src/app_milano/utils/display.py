import atexit
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path


CURRENT_DIR = Path(__file__).resolve().parent
SRC_DIR = Path(__file__).resolve().parents[2]
ROOT = SRC_DIR.parent

sys.path = [path for path in sys.path if path not in ("", str(CURRENT_DIR))]
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

import streamlit as st
import webview
from streamlit.runtime.scriptrunner_utils.script_run_context import get_script_run_ctx

from app_milano.config import Settings, load_env_file
from app_milano.utils.cache import CacheStore
from app_milano.utils.mongo import (
    close_mongo_context,
    create_mongo_context,
    get_mongo_source,
    get_ui_activity_series,
    get_ui_extended_conversation,
    get_ui_kpis,
    get_ui_longest_conversation_summary,
    get_ui_parent_tweet,
    get_ui_reply_tweets,
    get_ui_replies_for_tweet,
    get_ui_top_hashtags,
    get_ui_top_tweets,
    get_ui_tweet_by_id,
    get_ui_tweets_by_user,
    get_ui_user_by_id,
    get_ui_user_by_username,
    search_ui_hashtags,
    search_ui_tweets_by_text,
    search_ui_users,
)
from app_milano.utils.neo4j import (
    close_neo4j_context,
    create_neo4j_context,
    get_ui_q10_users_with_more_than_ten_followers,
    get_ui_q11_users_following_more_than_five_users,
    get_ui_q7_followers,
    get_ui_q8_following,
    get_ui_q9_mutual_connections,
)


PLACEHOLDER = "in progress"
ROUTES = ["Accueil", "Top 10", "Recherche", "Profil", "Hashtag", "Reponses", "Reseau"]


def print_connection_info(settings):
    print("Execution terminee.")
    print()
    print("MongoDB")
    print(settings.mongo_app_uri)
    print()
    print("Neo4j")
    print(f"Browser: {settings.neo4j_browser_url}")
    print(f"Bolt: {settings.neo4j_bolt_uri}")
    print(f"User: {settings.neo4j_user}")
    print(f"Password: {settings.neo4j_password}")


def print_question_results(results):
    print()
    print("Questions MongoDB")
    print(f"Q1 - Nombre d'utilisateurs : {results['user_count']}")
    print(f"Q2 - Nombre de tweets : {results['tweet_count']}")
    print(f"Q3 - Nombre de hashtags distincts : {results['distinct_hashtag_count']}")

    print()
    print("Q12 - Top 10 tweets les plus populaires")
    for index, tweet in enumerate(results["top_tweets"], start=1):
        text = " ".join(tweet["text"].split())
        preview = text[:90] + ("..." if len(text) > 90 else "")
        print(
            f"{index}. {tweet['tweet_id']} | @{tweet.get('username', 'unknown')} | "
            f"{tweet['favorite_count']} likes | {preview}"
        )

    print()
    print("Q13 - Top 10 hashtags les plus populaires")
    for index, hashtag in enumerate(results["top_hashtags"], start=1):
        print(f"{index}. #{hashtag['hashtag']} | {hashtag['tweet_count']} tweets")


def init_state():
    defaults = {
        "route": "Accueil",
        "transition_direction": "none",
        "selected_user_id": "",
        "selected_username": "",
        "selected_hashtag": "",
        "selected_tweet_id": "",
        "search_mode": "Utilisateur",
        "search_query": "",
        "ui_cache_store": None,
        "ui_mongo_context": None,
        "ui_neo4j_context": None,
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def get_ui_cache():
    cache = st.session_state.get("ui_cache_store")
    if cache is None:
        cache = CacheStore(ttl_seconds=45, max_items=512)
        st.session_state.ui_cache_store = cache
    return cache


def clear_ui_cache():
    get_ui_cache().clear()


def get_mongo_context():
    context = st.session_state.get("ui_mongo_context")
    if context is None:
        context = create_mongo_context(PLACEHOLDER)
        st.session_state.ui_mongo_context = context
    return context


def get_neo4j_context():
    context = st.session_state.get("ui_neo4j_context")
    if context is None:
        context = create_neo4j_context(PLACEHOLDER)
        st.session_state.ui_neo4j_context = context
    return context


def reset_ui_backends():
    mongo_context = st.session_state.get("ui_mongo_context")
    neo4j_context = st.session_state.get("ui_neo4j_context")
    if mongo_context is not None:
        close_mongo_context(mongo_context)
    if neo4j_context is not None:
        close_neo4j_context(neo4j_context)
    st.session_state.ui_mongo_context = None
    st.session_state.ui_neo4j_context = None
    clear_ui_cache()


def get_cached_mongo(repo, cache_name, producer, *key_parts):
    key = ("mongo", get_mongo_source(repo), cache_name) + tuple(key_parts)
    return get_ui_cache().get_or_set(key, producer)


def get_cached_neo4j(cache_name, producer, *key_parts):
    key = ("neo4j", cache_name) + tuple(key_parts)
    return get_ui_cache().get_or_set(key, producer)


def set_route(route):
    current_route = st.session_state.route
    if route not in ROUTES:
        st.session_state.route = route
        st.session_state.transition_direction = "none"
        return
    current_index = ROUTES.index(current_route)
    next_index = ROUTES.index(route)
    st.session_state.transition_direction = "forward" if next_index >= current_index else "backward"
    st.session_state.route = route


def move_route(delta):
    current_index = ROUTES.index(st.session_state.route)
    next_index = max(0, min(len(ROUTES) - 1, current_index + delta))
    st.session_state.transition_direction = "forward" if next_index >= current_index else "backward"
    st.session_state.route = ROUTES[next_index]


def open_profile(user_id="", username=""):
    st.session_state.selected_user_id = user_id or ""
    st.session_state.selected_username = username or ""
    set_route("Profil")


def open_hashtag(hashtag):
    st.session_state.selected_hashtag = hashtag or ""
    set_route("Hashtag")


def open_replies(tweet_id):
    st.session_state.selected_tweet_id = tweet_id or ""
    set_route("Reponses")


def open_search(mode, query):
    st.session_state.search_mode = mode
    st.session_state.search_query = query or ""
    set_route("Recherche")


def find_free_port():
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_server(url, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=2):
                return
        except Exception:
            time.sleep(0.4)
    raise SystemExit("L'interface Streamlit n'a pas demarre a temps.")


def launch_desktop():
    port = find_free_port()
    url = f"http://127.0.0.1:{port}"
    env = os.environ.copy()
    env["APP_MILANO_UI_MODE"] = "desktop"
    streamlit_app = ROOT / "src" / "app_milano" / "utils" / "display.py"

    command = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(streamlit_app),
        "--server.headless",
        "true",
        "--server.port",
        str(port),
        "--browser.gatherUsageStats",
        "false",
    ]

    process = subprocess.Popen(
        command,
        cwd=ROOT,
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    def cleanup():
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()

    atexit.register(cleanup)
    wait_for_server(url)

    webview.create_window(
        "Milano Review",
        url,
        width=1500,
        height=940,
        min_size=(1120, 760),
        text_select=True,
        background_color="#050505",
    )
    webview.start()


def apply_styles(dev_mode=True, transition_direction="none"):
    sidebar_rules = """
        [data-testid="stSidebar"] {
            background: #0d0d0d;
        }
        [data-testid="stSidebar"] * {
            color: #f5ead9 !important;
        }
    """
    if not dev_mode:
        sidebar_rules = """
        [data-testid="stSidebar"] { display: none; }
        [data-testid="collapsedControl"] { display: none; }
        [data-testid="stHeader"] { display: none; }
        [data-testid="stToolbar"] { display: none; }
        [data-testid="stDecoration"] { display: none; }
        [data-testid="stStatusWidget"] { display: none; }
        #MainMenu { display: none; }
        header { display: none; }
        """

    animation_rule = "none"
    if transition_direction == "forward":
        animation_rule = "wrappedSlideForward 280ms cubic-bezier(0.22, 1, 0.36, 1)"
    elif transition_direction == "backward":
        animation_rule = "wrappedSlideBackward 280ms cubic-bezier(0.22, 1, 0.36, 1)"

    styles = """
        <style>
        html, body, [data-testid="stAppViewContainer"], [data-testid="stMain"], .main {
            margin-top: 0 !important;
            padding-top: 0 !important;
        }
        .block-container {
            max-width: 980px;
            padding-top: 0 !important;
            padding-bottom: 2rem;
            background: linear-gradient(180deg, rgba(16,16,16,0.98) 0%, rgba(6,6,6,0.98) 100%);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 34px;
            padding-left: 1.15rem !important;
            padding-right: 1.15rem !important;
            padding-bottom: 1rem !important;
            box-shadow: 0 30px 80px rgba(0, 0, 0, 0.45);
            min-height: 760px;
            margin-top: 0 !important;
            animation: __ANIMATION_RULE__;
        }
        .stApp {
            background:
                radial-gradient(circle at top, rgba(255, 39, 20, 0.16), transparent 24%),
                radial-gradient(circle at bottom right, rgba(128, 0, 255, 0.10), transparent 28%),
                linear-gradient(180deg, #0b0b0b 0%, #040404 100%);
            color: #f6ecde;
            font-family: "Trebuchet MS", "Segoe UI", sans-serif;
        }
        __SIDEBAR_RULES__
        section.main > div {
            padding-top: 0 !important;
        }
        .stButton > button {
            border-radius: 999px;
            border: 1px solid rgba(255,255,255,0.10);
            background: #e62117;
            color: white;
            font-weight: 800;
            min-height: 2.65rem;
            box-shadow: none;
        }
        .stButton > button:hover {
            border: 1px solid rgba(255,255,255,0.14);
            background: #cb1c15;
            color: white;
        }
        .stButton > button[kind="secondary"] {
            background: #171717;
        }
        .stTextInput input, .stSelectbox div[data-baseweb="select"] > div, .stTextInput textarea {
            border-radius: 16px !important;
            border: 1px solid rgba(255,255,255,0.10) !important;
            background: rgba(18,18,18,0.96) !important;
            color: #f6ecde !important;
        }
        [data-testid="stMarkdownContainer"] *,
        [data-testid="stCaptionContainer"] *,
        .stRadio *,
        .stSelectbox *,
        .stTextInput *,
        .st-emotion-cache-1tacg1d *,
        .st-emotion-cache-1sdfa05 * {
            color: #f6ecde !important;
        }
        .stRadio label, .stSelectbox label, .stTextInput label {
            color: #bfae99 !important;
            font-weight: 700 !important;
        }
        .stMarkdown, .stCaption, p, li, label {
            color: #e8dccb;
        }
        [data-testid="stVegaLiteChart"] > div,
        [data-testid="stVegaLiteChart"] svg,
        [data-testid="stVegaLiteChart"] canvas {
            background: transparent !important;
        }
        [data-testid="stElementToolbar"] {
            background: transparent !important;
        }
        .wrapped-head {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 1rem;
            margin-bottom: 0.8rem;
        }
        .wrapped-brand {
            display: inline-flex;
            align-items: center;
            gap: 0.55rem;
            font-size: 0.94rem;
            font-weight: 800;
            color: #fff4e6;
        }
        .wrapped-logo {
            display: inline-flex;
            align-items: center;
            justify-content: center;
            width: 36px;
            height: 36px;
            border-radius: 50%;
            background: linear-gradient(135deg, #00c2ff 0%, #56e39f 48%, #ffffff 100%);
            color: #091018;
            font-size: 0.95rem;
            font-weight: 900;
            box-shadow: 0 0 0 2px rgba(255,255,255,0.08);
        }
        .wrapped-brand-text {
            display: flex;
            flex-direction: column;
            line-height: 1.02;
        }
        .wrapped-brand-title {
            color: #fff4e6;
            font-size: 0.92rem;
            font-weight: 900;
            letter-spacing: 0.02em;
        }
        .wrapped-brand-subtitle {
            color: #bfae99;
            font-size: 0.70rem;
            text-transform: uppercase;
            letter-spacing: 0.14em;
        }
        .wrapped-close {
            color: #c8b6a3;
            font-size: 1.2rem;
            font-weight: 700;
        }
        .wrapped-progress {
            display: grid;
            grid-template-columns: repeat(7, 1fr);
            gap: 0.35rem;
            margin-bottom: 1.25rem;
        }
        .wrapped-progress-item {
            height: 4px;
            border-radius: 999px;
            background: rgba(255,255,255,0.10);
        }
        .wrapped-progress-item.active {
            background: linear-gradient(90deg, #ff3d24 0%, #ff7462 100%);
        }
        .wrapped-title {
            font-size: 3.35rem;
            line-height: 0.94;
            font-weight: 900;
            text-transform: uppercase;
            letter-spacing: -0.04em;
            color: #fff4e6;
            margin: 0 0 0.5rem 0;
        }
        .wrapped-subtitle {
            color: #bfae99;
            font-size: 1.05rem;
            margin-bottom: 1rem;
            max-width: 34rem;
        }
        .wrapped-route-label {
            text-transform: uppercase;
            letter-spacing: 0.18em;
            font-size: 0.72rem;
            color: #9a8a78;
            margin-bottom: 0.6rem;
        }
        .wrapped-panel {
            background: rgba(255,255,255,0.03);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 24px;
            padding: 1rem;
            margin-bottom: 0.9rem;
        }
        .wrapped-stat {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 24px;
            padding: 1rem;
            min-height: 132px;
        }
        .wrapped-stat-label {
            color: #bfae99;
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.72rem;
            margin-bottom: 0.45rem;
        }
        .wrapped-stat-value {
            font-size: 2rem;
            font-weight: 900;
            color: #fff4e6;
            margin-bottom: 0.3rem;
        }
        .wrapped-stat-note {
            color: #bfae99;
            font-size: 0.9rem;
        }
        .wrapped-number-row {
            display: grid;
            grid-template-columns: 38px 1fr 90px;
            gap: 0.8rem;
            align-items: center;
            padding: 0.8rem 0;
            border-bottom: 1px solid rgba(255,255,255,0.08);
        }
        .wrapped-number-row:last-child {
            border-bottom: 0;
        }
        .wrapped-rank {
            font-size: 1.55rem;
            font-weight: 900;
            color: #fff4e6;
            text-align: center;
        }
        .wrapped-item-label {
            color: #fff4e6;
            font-weight: 700;
            margin-bottom: 0.3rem;
        }
        .wrapped-item-meta {
            color: #bfae99;
            font-size: 0.9rem;
        }
        .wrapped-bar-track {
            width: 100%;
            height: 3px;
            background: rgba(255,255,255,0.14);
            border-radius: 999px;
            position: relative;
            margin-top: 0.55rem;
        }
        .wrapped-bar-fill {
            height: 3px;
            border-radius: 999px;
            background: linear-gradient(90deg, #ff321e 0%, #ff7a5c 100%);
        }
        .wrapped-value-tag {
            font-weight: 800;
            color: #ffd5ce;
            text-align: right;
        }
        .wrapped-tweet {
            background: rgba(255,255,255,0.04);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: 24px;
            padding: 1rem 1rem 0.8rem 1rem;
            margin-bottom: 0.8rem;
        }
        .wrapped-tweet-meta {
            font-size: 0.84rem;
            color: #bfae99;
            margin-bottom: 0.55rem;
        }
        .wrapped-tweet-text {
            color: #fff4e6;
            font-size: 1.05rem;
            line-height: 1.45;
            margin-bottom: 0.65rem;
        }
        .wrapped-chip {
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 999px;
            background: rgba(255, 77, 58, 0.12);
            border: 1px solid rgba(255, 77, 58, 0.22);
            color: #ffb6ab;
            font-size: 0.82rem;
            font-weight: 700;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
        }
        .wrapped-placeholder {
            border: 1px dashed rgba(255,255,255,0.18);
            border-radius: 20px;
            background: rgba(255,255,255,0.03);
            color: #bfae99;
            text-align: center;
            padding: 2rem 1rem;
            font-weight: 800;
            letter-spacing: 0.14em;
            text-transform: uppercase;
        }
        .wrapped-hero-center-block {
            min-height: 430px;
            display: flex;
            align-items: center;
            justify-content: center;
            text-align: center;
        }
        .wrapped-hero-center-block .wrapped-title {
            max-width: 13rem;
            margin: 0 auto 1rem auto;
        }
        .wrapped-section-title {
            font-size: 1.1rem;
            font-weight: 900;
            color: #fff4e6;
            margin: 0.1rem 0 0.85rem 0;
        }
        .wrapped-foot {
            margin-top: 1rem;
            padding-top: 1rem;
            border-top: 1px solid rgba(255,255,255,0.08);
        }
        .wrapped-foot-note {
            color: #8f8173;
            text-align: center;
            font-size: 0.8rem;
            margin-top: 0.55rem;
        }
        .wrapped-pill {
            display: inline-block;
            padding: 0.35rem 0.75rem;
            border-radius: 999px;
            background: rgba(255,255,255,0.05);
            border: 1px solid rgba(255,255,255,0.10);
            color: #f6ecde;
            font-size: 0.82rem;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
        }
        .wrapped-divider {
            height: 1px;
            background: rgba(255,255,255,0.08);
            margin: 0.85rem 0;
        }
        .wrapped-grid-2 {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 0.9rem;
        }
        .wrapped-grid-3 {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 0.9rem;
        }
        .wrapped-grid-4 {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 0.9rem;
        }
        .st-key-edge-left-nav,
        .st-key-edge-right-nav {
            position: fixed;
            top: 0;
            bottom: 0;
            width: max(84px, calc((100vw - 980px) / 2));
            z-index: 25;
        }
        .st-key-edge-left-nav {
            left: 0;
        }
        .st-key-edge-right-nav {
            right: 0;
        }
        .st-key-edge-left-nav .stButton,
        .st-key-edge-right-nav .stButton {
            height: 100%;
        }
        .st-key-edge-left-nav button,
        .st-key-edge-right-nav button {
            width: 100%;
            min-height: 100vh !important;
            height: 100vh !important;
            border: 0 !important;
            border-radius: 0 !important;
            background: transparent !important;
            color: transparent !important;
            box-shadow: none !important;
        }
        .st-key-edge-left-nav button:hover {
            background: rgba(255,255,255,0.02) !important;
        }
        .st-key-edge-right-nav button:hover {
            background: rgba(255,255,255,0.02) !important;
        }
        .st-key-edge-left-nav button:disabled,
        .st-key-edge-right-nav button:disabled {
            background: transparent !important;
            opacity: 0.35;
        }
        @keyframes wrappedSlideForward {
            from {
                opacity: 0.40;
                transform: translateX(32px) scale(0.985);
            }
            to {
                opacity: 1;
                transform: translateX(0) scale(1);
            }
        }
        @keyframes wrappedSlideBackward {
            from {
                opacity: 0.40;
                transform: translateX(-32px) scale(0.985);
            }
            to {
                opacity: 1;
                transform: translateX(0) scale(1);
            }
        }
        @media (max-width: 980px) {
            .wrapped-grid-2,
            .wrapped-grid-3,
            .wrapped-grid-4 {
                grid-template-columns: 1fr;
            }
            .wrapped-title {
                font-size: 2.45rem;
            }
            .block-container {
                min-height: auto;
            }
            .st-key-edge-left-nav,
            .st-key-edge-right-nav {
                width: 52px;
            }
        }
        </style>
    """.replace("__SIDEBAR_RULES__", sidebar_rules).replace("__ANIMATION_RULE__", animation_rule)
    st.markdown(styles, unsafe_allow_html=True)


def render_dev_sidebar(repo):
    st.sidebar.markdown("## Milano")
    st.sidebar.caption("Mode dev de l'interface review")
    selected = st.sidebar.radio("Slide", ROUTES, index=ROUTES.index(st.session_state.route))
    if selected != st.session_state.route:
        st.session_state.route = selected
        st.rerun()
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Source active : {get_mongo_source(repo)}")
    if st.sidebar.button("Refresh data", use_container_width=True):
        reset_ui_backends()
        st.rerun()
    st.sidebar.caption("Les zones non branchees affichent `in progress`.")


def render_edge_navigation():
    current_index = ROUTES.index(st.session_state.route)
    if st.button(" ", key="edge-left-nav", disabled=current_index == 0):
        move_route(-1)
        st.rerun()
    if st.button(" ", key="edge-right-nav", disabled=current_index == len(ROUTES) - 1):
        move_route(1)
        st.rerun()


def render_progress(current_route):
    current_index = ROUTES.index(current_route)
    segments = []
    for index, _ in enumerate(ROUTES):
        css = "wrapped-progress-item"
        if index <= current_index:
            css += " active"
        segments.append(f"<div class='{css}'></div>")
    st.markdown(f"<div class='wrapped-progress'>{''.join(segments)}</div>", unsafe_allow_html=True)


def render_slide_header(current_route, title, subtitle):
    slide_index = ROUTES.index(current_route) + 1
    st.markdown(
        f"""
        <div class="wrapped-head">
            <div class="wrapped-brand">
                <span class="wrapped-logo">26</span>
                <span class="wrapped-brand-text">
                    <span class="wrapped-brand-title">Milano Cortina</span>
                    <span class="wrapped-brand-subtitle">Winter Games Review</span>
                </span>
            </div>
            <div class="wrapped-close">{slide_index:02d}/{len(ROUTES):02d}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    render_progress(current_route)
    st.markdown(f"<div class='wrapped-route-label'>{current_route}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='wrapped-title'>{title}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='wrapped-subtitle'>{subtitle}</div>", unsafe_allow_html=True)


def render_slide_footer(current_route):
    return None


def render_placeholder(placeholder):
    st.markdown(f"<div class='wrapped-placeholder'>{placeholder}</div>", unsafe_allow_html=True)


def render_stat(label, value, note):
    st.markdown(
        f"""
        <div class="wrapped-stat">
            <div class="wrapped-stat-label">{label}</div>
            <div class="wrapped-stat-value">{value}</div>
            <div class="wrapped-stat-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_rank_rows(items, label_key, value_key, placeholder, on_open=None, key_prefix="rank"):
    if items == placeholder:
        render_placeholder(placeholder)
        return
    if not items:
        render_placeholder(placeholder)
        return

    max_value = max(item.get(value_key, 0) for item in items) or 1
    for index, item in enumerate(items[:5], start=1):
        label = item.get(label_key, placeholder)
        value = item.get(value_key, placeholder)
        if isinstance(value, int):
            width = int((value / max_value) * 100) if max_value else 0
            bar = f"""
                <div class="wrapped-bar-track">
                    <div class="wrapped-bar-fill" style="width:{max(8, width)}%"></div>
                </div>
            """
        else:
            bar = ""

        cols = st.columns([0.22, 1.85, 0.5, 0.5])
        cols[0].markdown(f"<div class='wrapped-rank'>{index}</div>", unsafe_allow_html=True)
        cols[1].markdown(
            f"""
            <div class="wrapped-item-label">{label}</div>
            <div class="wrapped-item-meta">{bar}</div>
            """,
            unsafe_allow_html=True,
        )
        cols[2].markdown(f"<div class='wrapped-value-tag'>{value}</div>", unsafe_allow_html=True)
        if on_open:
            if cols[3].button("Voir", key=f"{key_prefix}-{index}-{label}"):
                on_open(item)
                st.rerun()


def render_tweet_card(tweet, placeholder, on_open_profile, on_open_replies, key_prefix="tweet", show_reply_button=True):
    if not tweet:
        render_placeholder(placeholder)
        return

    hashtags = tweet.get("hashtags", [])
    chips = "".join(f"<span class='wrapped-chip'>#{tag}</span>" for tag in hashtags[:4])
    st.markdown(
        f"""
        <div class="wrapped-tweet">
            <div class="wrapped-tweet-meta">@{tweet.get('username', 'unknown')} • {tweet.get('created_at', placeholder)} • {tweet.get('favorite_count', placeholder)} likes</div>
            <div class="wrapped-tweet-text">{tweet.get('text', placeholder)}</div>
            <div>{chips}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    buttons = st.columns([1, 1, 2])
    if buttons[0].button("Profil", key=f"{key_prefix}-profile-{tweet.get('tweet_id', 'x')}"):
        on_open_profile(user_id=tweet.get("user_id", ""), username=tweet.get("username", ""))
        st.rerun()
    if show_reply_button and buttons[1].button("Slide", key=f"{key_prefix}-reply-{tweet.get('tweet_id', 'x')}"):
        on_open_replies(tweet.get("tweet_id", ""))
        st.rerun()


def render_question_block(question_label, title, result, placeholder):
    st.markdown(f"<div class='wrapped-section-title'>{question_label} · {title}</div>", unsafe_allow_html=True)
    if result == placeholder:
        render_placeholder(placeholder)
        return
    if isinstance(result, list):
        if not result:
            render_placeholder(placeholder)
            return
        for item in result[:8]:
            if isinstance(item, dict):
                fragments = []
                for key, value in item.items():
                    fragments.append(f"{key}: {value}")
                st.markdown(f"<div class='wrapped-panel'>{' | '.join(fragments)}</div>", unsafe_allow_html=True)
            else:
                st.markdown(f"<div class='wrapped-panel'>{item}</div>", unsafe_allow_html=True)
        return
    if isinstance(result, dict):
        if not result:
            render_placeholder(placeholder)
            return
        for key, value in result.items():
            st.markdown(f"<div class='wrapped-panel'><strong>{key}</strong> : {value}</div>", unsafe_allow_html=True)
        return
    st.markdown(f"<div class='wrapped-panel'>{result}</div>", unsafe_allow_html=True)


def render_dark_activity_chart(activity_rows):
    values = []
    for index, row in enumerate(activity_rows):
        values.append(
            {
                "index": index + 1,
                "label": row.get("day", "")[5:],
                "tweet_count": row.get("tweet_count", 0),
            }
        )

    spec = {
        "$schema": "https://vega.github.io/schema/vega-lite/v5.json",
        "background": "transparent",
        "height": 170,
        "data": {"values": values},
        "mark": {
            "type": "area",
            "line": {"color": "#ff6b57", "strokeWidth": 2},
            "color": {
                "x1": 1,
                "y1": 1,
                "x2": 1,
                "y2": 0,
                "gradient": "linear",
                "stops": [
                    {"offset": 0, "color": "rgba(255, 107, 87, 0.08)"},
                    {"offset": 1, "color": "rgba(255, 61, 36, 0.55)"},
                ],
            },
        },
        "encoding": {
            "x": {
                "field": "label",
                "type": "ordinal",
                "axis": {
                    "labelColor": "#cdb8a3",
                    "title": None,
                    "domain": False,
                    "tickColor": "rgba(255,255,255,0.10)",
                    "labelPadding": 10,
                },
            },
            "y": {
                "field": "tweet_count",
                "type": "quantitative",
                "axis": {
                    "labelColor": "#cdb8a3",
                    "title": None,
                    "domain": False,
                    "gridColor": "rgba(255,255,255,0.08)",
                    "tickColor": "rgba(255,255,255,0.10)",
                },
            },
            "tooltip": [
                {"field": "label", "type": "ordinal", "title": "Jour"},
                {"field": "tweet_count", "type": "quantitative", "title": "Tweets"},
            ],
        },
        "config": {
            "view": {"stroke": None},
            "axis": {"labelFontSize": 12},
        },
    }
    st.vega_lite_chart(spec, use_container_width=True)


def render_home(repo, placeholder):
    kpis = get_cached_mongo(repo, "home-kpis", lambda: get_ui_kpis(repo))
    top_tweets = get_cached_mongo(repo, "home-top-tweets", lambda: get_ui_top_tweets(repo))
    top_hashtags = get_cached_mongo(repo, "home-top-hashtags", lambda: get_ui_top_hashtags(repo))
    activity = get_cached_mongo(repo, "home-activity", lambda: get_ui_activity_series(repo))

    render_slide_header("Accueil", "Milano 2026", "Recap interactif des temps forts, comptes suivis et conversations autour des JO d'hiver Milano Cortina 2026.")

    st.markdown(
        """
        <div class="wrapped-hero-center-block">
            <div>
                <div class="wrapped-route-label">Milano year review</div>
                <div class="wrapped-title">2026 Milano Review</div>
                <div class="wrapped-subtitle">Revivez les moments marquants de Milano Cortina 2026 a travers les tweets, hashtags phares et discussions les plus actives.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    stat_cols = st.columns(3)
    with stat_cols[0]:
        render_stat("Utilisateurs", kpis.get("user_count", placeholder), "Q1")
    with stat_cols[1]:
        render_stat("Tweets", kpis.get("tweet_count", placeholder), "Q2")
    with stat_cols[2]:
        render_stat("Hashtags", kpis.get("distinct_hashtag_count", placeholder), "Q3")

    st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
    grid_cols = st.columns(2)
    with grid_cols[0]:
        st.markdown("<div class='wrapped-section-title'>Moment fort</div>", unsafe_allow_html=True)
        if top_tweets:
            render_tweet_card(top_tweets[0], placeholder, open_profile, open_replies, key_prefix="home-top")
        else:
            render_placeholder(placeholder)
    with grid_cols[1]:
        st.markdown("<div class='wrapped-section-title'>Hashtag phare</div>", unsafe_allow_html=True)
        if top_hashtags:
            render_rank_rows(
                top_hashtags[:5],
                "hashtag",
                "tweet_count",
                placeholder,
                on_open=lambda item: open_hashtag(item.get("hashtag", "")),
                key_prefix="home-hashtags",
            )
        else:
            render_placeholder(placeholder)

    st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='wrapped-section-title'>Activite recente</div>", unsafe_allow_html=True)
    if activity:
        render_dark_activity_chart(activity)
    else:
        render_placeholder(placeholder)

    render_slide_footer("Accueil")


def render_top10(repo, placeholder):
    render_slide_header("Top 10", "Top content", "Les tweets et hashtags les plus marquants du dataset.")

    mode = st.radio("Classement", ["Tweets", "Hashtags"], horizontal=True)

    if mode == "Tweets":
        items = get_cached_mongo(repo, "top10-tweets", lambda: get_ui_top_tweets(repo))
        st.markdown("<div class='wrapped-section-title'>Top tweets</div>", unsafe_allow_html=True)
        render_rank_rows(
            items,
            "tweet_id",
            "favorite_count",
            placeholder,
            on_open=lambda item: open_replies(item.get("tweet_id", "")),
            key_prefix="top10-tweets",
        )
        if items and items != placeholder:
            st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
            render_tweet_card(items[0], placeholder, open_profile, open_replies, key_prefix="top10-feature")
    else:
        items = get_cached_mongo(repo, "top10-hashtags", lambda: get_ui_top_hashtags(repo))
        st.markdown("<div class='wrapped-section-title'>Top hashtags</div>", unsafe_allow_html=True)
        render_rank_rows(
            items,
            "hashtag",
            "tweet_count",
            placeholder,
            on_open=lambda item: open_hashtag(item.get("hashtag", "")),
            key_prefix="top10-hashtags",
        )

    st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='wrapped-section-title'>Discussion dominante</div>", unsafe_allow_html=True)
    conversation = get_cached_mongo(repo, "top10-longest-conversation", lambda: get_ui_longest_conversation_summary(repo))
    if not conversation:
        render_placeholder(placeholder)
    else:
        summary_cols = st.columns(3)
        with summary_cols[0]:
            render_stat("Taille", conversation.get("conversation_size", placeholder), "Q15")
        with summary_cols[1]:
            render_stat("Chaine max", conversation.get("longest_reply_chain_length", placeholder), "profondeur")
        with summary_cols[2]:
            render_stat("Fins", conversation.get("ending_tweet_count", placeholder), "branches")
        start_tweet = conversation.get("start_tweet")
        if start_tweet:
            render_tweet_card(start_tweet, placeholder, open_profile, open_replies, key_prefix="top10-conversation")

    render_slide_footer("Top 10")


def render_search(repo, placeholder):
    render_slide_header("Recherche", "Find anything", "Recherche utilisateur, hashtag ou texte dans le style d'une slide interactive.")

    controls = st.columns([1, 1.8, 0.7])
    mode = controls[0].selectbox("Mode", ["Utilisateur", "Hashtag", "Texte"], index=["Utilisateur", "Hashtag", "Texte"].index(st.session_state.search_mode))
    query = controls[1].text_input("Recherche", value=st.session_state.search_query, placeholder="username, hashtag ou texte")
    st.session_state.search_mode = mode
    st.session_state.search_query = query
    if controls[2].button("Go", use_container_width=True):
        st.rerun()

    if not query.strip():
        render_placeholder("Type a search")
        render_slide_footer("Recherche")
        return

    if mode == "Utilisateur":
        rows = get_cached_mongo(repo, "search-users", lambda: search_ui_users(repo, query), mode, query.strip().lower())
        if rows == placeholder or not rows:
            render_placeholder(placeholder)
        else:
            st.markdown("<div class='wrapped-section-title'>Resultats utilisateurs</div>", unsafe_allow_html=True)
            for user in rows[:6]:
                cols = st.columns([2.4, 0.7])
                cols[0].markdown(
                    f"<div class='wrapped-panel'><strong>@{user.get('username', placeholder)}</strong><br>{user.get('role', placeholder)} · {user.get('country', placeholder)}</div>",
                    unsafe_allow_html=True,
                )
                if cols[1].button("Voir", key=f"search-user-{user.get('user_id', '')}"):
                    open_profile(user_id=user.get("user_id", ""), username=user.get("username", ""))
                    st.rerun()
    elif mode == "Hashtag":
        rows = get_cached_mongo(repo, "search-hashtags", lambda: search_ui_hashtags(repo, query), mode, query.strip().lower())
        st.markdown("<div class='wrapped-section-title'>Resultats hashtags</div>", unsafe_allow_html=True)
        render_rank_rows(
            rows,
            "hashtag",
            "tweet_count",
            placeholder,
            on_open=lambda item: open_hashtag(item.get("hashtag", "")),
            key_prefix="search-hashtags",
        )
    else:
        rows = get_cached_mongo(repo, "search-text", lambda: search_ui_tweets_by_text(repo, query), mode, query.strip().lower())
        if rows == placeholder or not rows:
            render_placeholder(placeholder)
        else:
            st.markdown("<div class='wrapped-section-title'>Resultats tweets</div>", unsafe_allow_html=True)
            for tweet in rows[:6]:
                render_tweet_card(tweet, placeholder, open_profile, open_replies, key_prefix=f"search-{tweet.get('tweet_id', 'x')}")

    render_slide_footer("Recherche")


def render_profile(repo, placeholder):
    render_slide_header("Profil", "Creator focus", "Lecture profilee d'un compte et de ses tweets recents.")

    selected_user = None
    if st.session_state.selected_username:
        selected_user = get_cached_mongo(
            repo,
            "profile-by-username",
            lambda: get_ui_user_by_username(repo, st.session_state.selected_username),
            st.session_state.selected_username.lower(),
        )
    if not selected_user and st.session_state.selected_user_id:
        selected_user = get_cached_mongo(
            repo,
            "profile-by-id",
            lambda: get_ui_user_by_id(repo, st.session_state.selected_user_id),
            st.session_state.selected_user_id,
        )

    if not selected_user:
        lookup = st.text_input("Username exact", placeholder="MilanoOps")
        if st.button("Afficher", use_container_width=True):
            open_profile(username=lookup)
            st.rerun()
        render_placeholder("Select a profile")
        render_slide_footer("Profil")
        return

    st.markdown(
        f"""
        <div class="wrapped-panel">
            <div class="wrapped-route-label">Compte</div>
            <div class="wrapped-title" style="font-size:2.3rem; text-transform:none;">@{selected_user.get('username', placeholder)}</div>
            <div class="wrapped-subtitle">{selected_user.get('role', placeholder)} · {selected_user.get('country', placeholder)} · cree le {selected_user.get('created_at', placeholder)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    tweets = get_cached_mongo(
        repo,
        "profile-tweets",
        lambda: get_ui_tweets_by_user(repo, selected_user.get("user_id", "")),
        selected_user.get("user_id", ""),
    )
    st.markdown("<div class='wrapped-section-title'>Ses tweets recents</div>", unsafe_allow_html=True)
    if tweets == placeholder or not tweets:
        render_placeholder(placeholder)
    else:
        for tweet in tweets[:5]:
            render_tweet_card(tweet, placeholder, open_profile, open_replies, key_prefix=f"profile-{tweet.get('tweet_id', 'x')}")

    render_slide_footer("Profil")


def render_hashtag(placeholder):
    render_slide_header("Hashtag", "Hashtag spotlight", "Espace reserve aux questions 4 et 5. Tant que ce n'est pas branche, la slide reste en attente.")
    render_placeholder(placeholder)
    render_slide_footer("Hashtag")


def render_replies(repo, placeholder):
    render_slide_header("Reponses", "Thread review", "Question 6 pour la liste globale, puis detail de conversation pour les threads.")

    st.markdown("<div class='wrapped-section-title'>Q6 · Tous les tweets qui sont des reponses</div>", unsafe_allow_html=True)
    reply_tweets = get_cached_mongo(repo, "reply-tweets", lambda: get_ui_reply_tweets(repo))
    if reply_tweets == placeholder or not reply_tweets:
        render_placeholder(placeholder)
    else:
        for tweet in reply_tweets[:5]:
            render_tweet_card(tweet, placeholder, open_profile, open_replies, key_prefix=f"reply-list-{tweet.get('tweet_id', 'x')}")

    st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='wrapped-section-title'>Selection d'un tweet</div>", unsafe_allow_html=True)
    manual = st.text_input("Tweet ID", value=st.session_state.selected_tweet_id, placeholder="T0001")
    if manual != st.session_state.selected_tweet_id:
        st.session_state.selected_tweet_id = manual

    if not st.session_state.selected_tweet_id.strip():
        render_placeholder("Select a reply")
        render_slide_footer("Reponses")
        return

    tweet_id = st.session_state.selected_tweet_id
    tweet = get_cached_mongo(repo, "reply-selected", lambda: get_ui_tweet_by_id(repo, tweet_id), tweet_id)
    if not tweet:
        render_placeholder(placeholder)
        render_slide_footer("Reponses")
        return

    grid_cols = st.columns(2)
    with grid_cols[0]:
        st.markdown("<div class='wrapped-section-title'>Tweet selectionne</div>", unsafe_allow_html=True)
        render_tweet_card(tweet, placeholder, open_profile, open_replies, key_prefix=f"reply-focus-{tweet_id}", show_reply_button=False)
        st.markdown("<div class='wrapped-section-title'>Tweet parent</div>", unsafe_allow_html=True)
        parent = get_cached_mongo(
            repo,
            "reply-parent",
            lambda: get_ui_parent_tweet(repo, tweet),
            tweet.get("tweet_id", ""),
            tweet.get("in_reply_to_tweet_id", ""),
        )
        if tweet.get("in_reply_to_tweet_id") and parent:
            render_tweet_card(parent, placeholder, open_profile, open_replies, key_prefix=f"reply-parent-{tweet_id}", show_reply_button=False)
        else:
            render_placeholder(placeholder)

    with grid_cols[1]:
        st.markdown("<div class='wrapped-section-title'>Reponses directes</div>", unsafe_allow_html=True)
        replies = get_cached_mongo(
            repo,
            "reply-children",
            lambda: get_ui_replies_for_tweet(repo, tweet.get("tweet_id", "")),
            tweet.get("tweet_id", ""),
        )
        if replies == placeholder or not replies:
            render_placeholder(placeholder)
        else:
            for reply in replies[:4]:
                render_tweet_card(reply, placeholder, open_profile, open_replies, key_prefix=f"reply-child-{reply.get('tweet_id', 'x')}", show_reply_button=False)

    st.markdown("<div class='wrapped-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='wrapped-section-title'>Conversation etendue</div>", unsafe_allow_html=True)
    conversation = get_cached_mongo(
        repo,
        "reply-conversation",
        lambda: get_ui_extended_conversation(repo, tweet.get("tweet_id", "")),
        tweet.get("tweet_id", ""),
    )
    if not conversation:
        render_placeholder(placeholder)
    else:
        stat_cols = st.columns(4)
        with stat_cols[0]:
            render_stat("Taille", conversation.get("conversation_size", placeholder), "Q14/Q16")
        with stat_cols[1]:
            render_stat("Chaine", conversation.get("longest_reply_chain_length", placeholder), "Q15")
        with stat_cols[2]:
            render_stat("Directes", conversation.get("direct_reply_count", placeholder), "a partir du debut")
        with stat_cols[3]:
            render_stat("Plus longue", "oui" if conversation.get("is_longest") else "non", "dataset global")

    render_slide_footer("Reponses")


def render_network(placeholder):
    render_slide_header("Reseau", "Social graph", "Questions 7 a 11 sur les relations de suivi dans Neo4j.")
    graph_context = get_neo4j_context()
    render_question_block("Q7", "Followers de MilanoOps", get_cached_neo4j("q7", lambda: get_ui_q7_followers(graph_context)), placeholder)
    render_question_block("Q8", "Utilisateurs suivis par MilanoOps", get_cached_neo4j("q8", lambda: get_ui_q8_following(graph_context)), placeholder)
    render_question_block("Q9", "Relations reciproques avec MilanoOps", get_cached_neo4j("q9", lambda: get_ui_q9_mutual_connections(graph_context)), placeholder)
    render_question_block("Q10", "Utilisateurs avec plus de 10 followers", get_cached_neo4j("q10", lambda: get_ui_q10_users_with_more_than_ten_followers(graph_context)), placeholder)
    render_question_block("Q11", "Utilisateurs qui suivent plus de 5 utilisateurs", get_cached_neo4j("q11", lambda: get_ui_q11_users_following_more_than_five_users(graph_context)), placeholder)
    render_slide_footer("Reseau")


def run_streamlit_ui():
    if get_script_run_ctx() is None:
        raise SystemExit(
            "Cette interface Streamlit doit etre lancee avec :\n"
            "python src/app_milano/main.py\n"
            "ou\n"
            "conda run -n cours streamlit run src/app_milano/utils/display.py"
        )

    st.set_page_config(page_title="Milano Review", page_icon="▶", layout="wide")
    load_env_file(required=False)
    init_state()
    transition_direction = st.session_state.transition_direction
    apply_styles(dev_mode=os.getenv("APP_MILANO_UI_MODE") != "desktop", transition_direction=transition_direction)
    st.session_state.transition_direction = "none"
    repo = get_mongo_context()

    try:
        if os.getenv("APP_MILANO_UI_MODE") != "desktop":
            render_dev_sidebar(repo)
        render_edge_navigation()

        route = st.session_state.route
        if route == "Accueil":
            render_home(repo, PLACEHOLDER)
        elif route == "Top 10":
            render_top10(repo, PLACEHOLDER)
        elif route == "Recherche":
            render_search(repo, PLACEHOLDER)
        elif route == "Profil":
            render_profile(repo, PLACEHOLDER)
        elif route == "Hashtag":
            render_hashtag(PLACEHOLDER)
        elif route == "Reponses":
            render_replies(repo, PLACEHOLDER)
        else:
            render_network(PLACEHOLDER)
    except Exception:
        st.error("Erreur de chargement de l'interface.")
        st.markdown("...........")


if __name__ == "__main__":
    run_streamlit_ui()
