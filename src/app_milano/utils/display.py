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


def print_connection_info(settings: Settings) -> None:
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


def print_question_results(results: dict) -> None:
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
        "selected_user_id": "",
        "selected_username": "",
        "selected_hashtag": "",
        "selected_tweet_id": "",
        "search_mode": "Utilisateur",
        "search_query": "",
    }
    for key, value in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = value


def set_route(route):
    st.session_state.route = route


def open_profile(user_id="", username=""):
    st.session_state.selected_user_id = user_id or ""
    st.session_state.selected_username = username or ""
    st.session_state.route = "Profil"


def open_hashtag(hashtag):
    st.session_state.selected_hashtag = hashtag or ""
    st.session_state.route = "Hashtag"


def open_replies(tweet_id):
    st.session_state.selected_tweet_id = tweet_id or ""
    st.session_state.route = "Reponses"


def open_search(mode, query):
    st.session_state.search_mode = mode
    st.session_state.search_query = query or ""
    st.session_state.route = "Recherche"


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
            time.sleep(0.5)
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
        "Milano Interface",
        url,
        width=1480,
        height=920,
        min_size=(1080, 720),
        text_select=True,
        background_color="#eef3f7",
    )
    webview.start()


def apply_styles(dev_mode=True):
    sidebar_rules = """
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #19324a 0%, #284e68 100%);
        }
        [data-testid="stSidebar"] * {
            color: #f7f2e8 !important;
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

    styles = """
        <style>
        .block-container {
            max-width: 1380px;
            padding-top: 0.6rem;
            padding-bottom: 2rem;
        }
        .stApp {
            background: #eef2f6;
            color: #22324b;
            font-family: "Trebuchet MS", "Segoe UI", sans-serif;
        }
        __SIDEBAR_RULES__
        .stButton > button {
            border-radius: 999px;
            border: 0;
            background: #6a98f5;
            color: white;
            font-weight: 700;
            min-height: 2.6rem;
            box-shadow: 0 10px 24px rgba(92, 140, 255, 0.22);
        }
        .stButton > button:hover {
            color: white;
            border: 0;
            background: #5a87e1;
        }
        .stTextInput input, .stSelectbox div[data-baseweb="select"] > div, .stTextInput textarea {
            border-radius: 16px !important;
            border: 1px solid rgba(34,50,75,0.08) !important;
            background: rgba(255,255,255,0.92) !important;
        }
        div[data-baseweb="select"] {
            border-radius: 16px;
        }
        .stRadio [role="radiogroup"] {
            gap: 0.75rem;
        }
        .milano-title {
            font-size: 2.3rem;
            font-weight: 800;
            letter-spacing: 0.04em;
            color: #263955;
            margin-bottom: 0.15rem;
        }
        .milano-subtitle {
            color: #6f7f95;
            margin-bottom: 1rem;
        }
        .milano-card {
            background: rgba(255,255,255,0.95);
            border: 1px solid rgba(34,50,75,0.07);
            border-radius: 24px;
            padding: 1rem 1rem 0.9rem 1rem;
            box-shadow: 0 18px 36px rgba(28, 39, 60, 0.08);
            min-height: 140px;
            margin-bottom: 0.9rem;
        }
        .milano-shell {
            background: rgba(255,255,255,0.72);
            border: 1px solid rgba(34,50,75,0.06);
            border-radius: 30px;
            box-shadow: 0 20px 50px rgba(32, 42, 68, 0.10);
            padding: 1.25rem;
            backdrop-filter: blur(10px);
        }
        .milano-topbar {
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(34,50,75,0.06);
            border-radius: 24px;
            padding: 0.9rem 1rem 1rem 1rem;
            margin-bottom: 0.8rem;
        }
        .milano-badge {
            display: inline-block;
            padding: 0.35rem 0.75rem;
            border-radius: 999px;
            background: rgba(82, 197, 190, 0.12);
            color: #2f7d78;
            font-size: 0.82rem;
            font-weight: 700;
            margin-left: 0.4rem;
        }
        .milano-metric {
            border-radius: 26px;
            padding: 1rem;
            color: white;
            box-shadow: 0 20px 38px rgba(52, 88, 163, 0.18);
            min-height: 148px;
            margin-bottom: 0.9rem;
        }
        .milano-metric.aqua {
            background: #6cb9f2;
        }
        .milano-metric.mint {
            background: #5fd6bf;
        }
        .milano-metric.violet {
            background: #8b73ef;
        }
        .milano-metric.indigo {
            background: #6e95f5;
        }
        .milano-metric .metric-icon {
            width: 42px;
            height: 42px;
            border-radius: 14px;
            display: flex;
            align-items: center;
            justify-content: center;
            background: rgba(255,255,255,0.2);
            font-size: 1.2rem;
            margin-bottom: 1.2rem;
        }
        .milano-metric .metric-title {
            font-size: 0.9rem;
            opacity: 0.9;
            margin-bottom: 0.3rem;
        }
        .milano-metric .metric-value {
            font-size: 2rem;
            font-weight: 800;
            margin-bottom: 0.25rem;
        }
        .milano-metric .metric-note {
            font-size: 0.85rem;
            opacity: 0.92;
        }
        .milano-panel {
            background: rgba(255,255,255,0.92);
            border: 1px solid rgba(34,50,75,0.06);
            border-radius: 24px;
            padding: 1rem 1rem 0.8rem 1rem;
            box-shadow: 0 16px 34px rgba(27, 42, 69, 0.08);
            margin-bottom: 0.95rem;
        }
        .milano-mini-card {
            background: rgba(246, 249, 252, 0.95);
            border: 1px solid rgba(34,50,75,0.06);
            border-radius: 18px;
            padding: 0.9rem;
            min-height: 128px;
            margin-bottom: 0.85rem;
        }
        .milano-nav-wrap {
            margin-bottom: 0.8rem;
        }
        .milano-eyebrow {
            text-transform: uppercase;
            letter-spacing: 0.12em;
            font-size: 0.72rem;
            color: #7a8aa2;
            margin-bottom: 0.35rem;
        }
        .milano-number {
            font-size: 2rem;
            font-weight: 800;
            color: #263955;
        }
        .milano-panel-title {
            font-size: 1.3rem;
            font-weight: 800;
            color: #24344e;
            margin: 0.2rem 0 0.75rem 0;
        }
        .milano-muted {
            color: #7b8a9e;
        }
        .milano-placeholder {
            border: 2px dashed #b55d46;
            background: rgba(181,93,70,0.08);
            color: #8a3f2c;
            border-radius: 14px;
            padding: 0.85rem 1rem;
            text-align: center;
            font-weight: 700;
            letter-spacing: 0.08em;
        }
        .milano-tweet {
            background: rgba(255,255,255,0.9);
            border-left: 5px solid #52c5be;
            border-radius: 18px;
            padding: 0.9rem 1rem;
            margin-bottom: 0.7rem;
            box-shadow: 0 12px 22px rgba(25,50,74,0.05);
        }
        .milano-meta {
            font-size: 0.85rem;
            color: #536173;
            margin-bottom: 0.4rem;
        }
        .milano-chip {
            display: inline-block;
            padding: 0.2rem 0.55rem;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
            border-radius: 999px;
            background: rgba(29,53,87,0.08);
            border: 1px solid rgba(29,53,87,0.18);
            color: #1d3557;
            font-size: 0.82rem;
            font-weight: 700;
        }
        .milano-bar-row {
            margin-bottom: 0.65rem;
        }
        .milano-bar-track {
            width: 100%;
            height: 12px;
            background: rgba(29,53,87,0.10);
            border-radius: 999px;
            overflow: hidden;
        }
        .milano-bar-fill {
            height: 12px;
            border-radius: 999px;
            background: linear-gradient(90deg, #1d3557 0%, #3e7c74 100%);
        }
        .milano-hero {
            background: rgba(255,255,255,0.96);
            border: 1px solid rgba(34,50,75,0.07);
            border-radius: 24px;
            padding: 1.1rem;
            margin-bottom: 1rem;
            box-shadow: 0 16px 34px rgba(27, 42, 69, 0.08);
        }
        .milano-divider {
            height: 1px;
            background: rgba(25,50,74,0.12);
            margin: 0.8rem 0;
        }
        .milano-user-row {
            display: flex;
            align-items: center;
            justify-content: space-between;
            gap: 0.75rem;
            padding: 0.8rem 0.1rem;
            border-bottom: 1px solid rgba(34,50,75,0.06);
        }
        .milano-user-row:last-child {
            border-bottom: 0;
        }
        .milano-avatar {
            width: 42px;
            height: 42px;
            border-radius: 50%;
            background: #7fa2f2;
            color: white;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 800;
        }
        .milano-chip-soft {
            display: inline-block;
            padding: 0.3rem 0.7rem;
            border-radius: 999px;
            background: rgba(92, 140, 255, 0.08);
            color: #4c63c7;
            font-size: 0.8rem;
            font-weight: 700;
            margin-right: 0.35rem;
            margin-bottom: 0.35rem;
        }
        .milano-label-row {
            display: flex;
            gap: 0.45rem;
            flex-wrap: wrap;
            margin-top: 0.6rem;
        }
        </style>
    """.replace("__SIDEBAR_RULES__", sidebar_rules)
    st.markdown(styles, unsafe_allow_html=True)


def render_sidebar(repo, routes, current_route, on_route_change):
    st.sidebar.markdown("## Milano")
    st.sidebar.caption("Interface Streamlit inspiree du croquis.")
    selected = st.sidebar.radio("Navigation", routes, index=routes.index(current_route))
    if selected != current_route:
        on_route_change(selected)
    st.sidebar.markdown("---")
    st.sidebar.caption(f"Source active : {get_mongo_source(repo)}")
    st.sidebar.caption("Si une donnee ou une question n'est pas encore finalisee, l'interface affiche `in progress`.")


def render_page_nav(on_route_change, current_route):
    st.markdown("<div class='milano-nav-wrap'>", unsafe_allow_html=True)
    nav_cols = st.columns([0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 0.8, 1.4])
    labels = ["Accueil", "Top 10", "Recherche", "Profil", "Hashtag", "Reponses", "Reseau"]
    for index, label in enumerate(labels):
        button_label = label
        if label == current_route:
            button_label = f"• {label}"
        if nav_cols[index].button(button_label, key=f"page-nav-{current_route}-{label}", use_container_width=True):
            on_route_change(label)
            st.rerun()
    st.markdown("</div>", unsafe_allow_html=True)


def render_metric_card(title, value, note, variant, icon):
    st.markdown(
        f"""
        <div class="milano-metric {variant}">
            <div class="metric-icon">{icon}</div>
            <div class="metric-title">{title}</div>
            <div class="metric-value">{value}</div>
            <div class="metric-note">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
def render_placeholder(placeholder):
    st.markdown(f"<div class='milano-placeholder'>{placeholder}</div>", unsafe_allow_html=True)


def render_page_header(title, subtitle):
    st.markdown(f"<div class='milano-title'>{title}</div>", unsafe_allow_html=True)
    st.markdown(f"<div class='milano-subtitle'>{subtitle}</div>", unsafe_allow_html=True)


def render_card(title, value, note="", kind=""):
    classes = "milano-card"
    if kind:
        classes += f" {kind}"
    st.markdown(
        f"""
        <div class="{classes}">
            <div class="milano-eyebrow">{title}</div>
            <div class="milano-number">{value}</div>
            <div class="milano-muted">{note}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_question_block(question_label, title, result, placeholder):
    st.markdown(f"<div class='milano-panel-title'>{question_label} - {title}</div>", unsafe_allow_html=True)

    if result == placeholder:
        render_placeholder(placeholder)
        return

    if isinstance(result, dict):
        for key, value in result.items():
            if isinstance(value, list):
                st.markdown(f"**{key}**")
                if not value:
                    render_placeholder(placeholder)
                else:
                    for item in value[:10]:
                        st.markdown(f"- `{item}`")
            else:
                st.markdown(f"**{key}** : {value}")
        return

    if isinstance(result, list):
        if not result:
            render_placeholder(placeholder)
            return
        for item in result[:10]:
            if isinstance(item, dict):
                fragments = []
                for key, value in item.items():
                    fragments.append(f"{key}: {value}")
                st.markdown(f"- {' | '.join(fragments)}")
            else:
                st.markdown(f"- {item}")
        return

    st.markdown(str(result))


def render_bar_rows(items, label_key, value_key, placeholder, on_click=None, key_prefix="bar"):
    if items == placeholder:
        render_placeholder(placeholder)
        return

    if not items:
        st.caption("Aucun resultat.")
        return

    max_value = max(item.get(value_key, 0) for item in items) or 1
    for index, item in enumerate(items, start=1):
        label = item.get(label_key, placeholder)
        value = item.get(value_key, placeholder)
        width = 0
        if isinstance(value, int):
            width = max(8, int((value / max_value) * 100))

        cols = st.columns([0.45, 2.6, 0.8, 0.9])
        cols[0].markdown(f"**{index}.**")
        cols[1].markdown(label)
        if isinstance(value, int):
            cols[2].markdown(
                f"""
                <div class="milano-bar-row">
                    <div class="milano-bar-track">
                        <div class="milano-bar-fill" style="width:{width}%"></div>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            cols[2].caption(str(value))
        else:
            cols[2].markdown(value)
        if on_click:
            if cols[3].button("Ouvrir", key=f"{key_prefix}-{index}-{label}"):
                on_click(item)
                st.rerun()


def render_tweet_card(tweet, placeholder, on_open_profile, on_open_replies, key_prefix="tweet", show_reply_button=True):
    if not tweet:
        render_placeholder(placeholder)
        return

    hashtags = tweet.get("hashtags", [])
    chips = "".join(f"<span class='milano-chip'>#{tag}</span>" for tag in hashtags[:5])
    st.markdown(
        f"""
        <div class="milano-tweet">
            <div class="milano-meta">@{tweet.get('username', 'unknown')} • {tweet.get('created_at', placeholder)} • {tweet.get('favorite_count', placeholder)} likes</div>
            <div>{tweet.get('text', placeholder)}</div>
            <div style="margin-top:0.55rem;">{chips}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    action_cols = st.columns([1, 1, 3])
    if action_cols[0].button("Profil", key=f"{key_prefix}-profile-{tweet.get('tweet_id', 'x')}"):
        on_open_profile(user_id=tweet.get("user_id", ""), username=tweet.get("username", ""))
        st.rerun()
    if show_reply_button and action_cols[1].button("Reponses", key=f"{key_prefix}-reply-{tweet.get('tweet_id', 'x')}"):
        on_open_replies(tweet.get("tweet_id", ""))
        st.rerun()


def render_home(repo, placeholder, on_open_search, on_open_profile, on_open_hashtag, on_open_replies, on_route_change):
    kpis = get_ui_kpis(repo)
    activity = get_ui_activity_series(repo)
    top_hashtags = get_ui_top_hashtags(repo)[:4]

    st.markdown("<div class='milano-shell'>", unsafe_allow_html=True)
    st.markdown(
        """
        <div class="milano-topbar">
            <div class="milano-eyebrow">Milano dashboard</div>
        """,
        unsafe_allow_html=True,
    )
    render_page_nav(on_route_change, "Accueil")
    st.markdown(
        """
            <div>
                <div class="milano-title" style="margin-bottom:0.1rem;">Control Board</div>
                <div class="milano-subtitle" style="margin-bottom:0;">Navigation rapide entre comptes, hashtags, tops et conversations.</div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    left_col, right_col = st.columns([1.35, 1], gap="large")

    with left_col:
        st.markdown("<div class='milano-panel-title'>Questions 1 a 3</div>", unsafe_allow_html=True)
        metric_rows = [st.columns(2, gap="medium"), st.columns(2, gap="medium")]
        metric_data = [
            ("Q1 Profils", kpis.get("user_count", placeholder), "nombre total d'utilisateurs", "aqua", "◉"),
            ("Q3 Hashtags", kpis.get("distinct_hashtag_count", placeholder), "hashtags distincts", "mint", "⌗"),
            ("Q2 Tweets", kpis.get("tweet_count", placeholder), "nombre total de tweets", "violet", "✦"),
            ("Acces", "5", "vues directes disponibles", "indigo", "➜"),
        ]
        for row_index, row in enumerate(metric_rows):
            for col_index, col in enumerate(row):
                title, value, note, variant, icon = metric_data[(row_index * 2) + col_index]
                with col:
                    render_metric_card(title, value, note, variant, icon)

        st.markdown("<div class='milano-hero'>", unsafe_allow_html=True)
        st.markdown("<div class='milano-panel-title'>Hub de recherche</div>", unsafe_allow_html=True)
        search_cols = st.columns([1.05, 1.65, 0.8])
        mode = search_cols[0].selectbox("Mode", ["Utilisateur", "Hashtag", "Texte"], key="home-search-mode")
        query = search_cols[1].text_input(
            "Recherche",
            key="home-search-query",
            placeholder="username, hashtag ou texte de tweet",
        )
        if search_cols[2].button("Explorer", use_container_width=True):
            on_open_search(mode, query)
            st.rerun()
        st.markdown(
            """
            <div class="milano-label-row">
                <span class="milano-chip-soft">Profil exact</span>
                <span class="milano-chip-soft">Hashtag direct</span>
                <span class="milano-chip-soft">Texte libre</span>
            </div>
            """,
            unsafe_allow_html=True,
        )
        st.markdown("</div>", unsafe_allow_html=True)

    with right_col:
        st.markdown("<div class='milano-panel-title'>Activity</div>", unsafe_allow_html=True)
        st.caption("Volume des tweets sur les derniers jours disponibles.")
        if activity:
            counts = [row.get("tweet_count", 0) for row in activity]
            st.area_chart({"Tweets": counts}, height=220)
            labels = " · ".join(row.get("day", "")[5:] for row in activity)
            st.caption(labels)
        else:
            render_placeholder(placeholder)

        st.markdown("<div class='milano-panel-title'>Trending hashtags</div>", unsafe_allow_html=True)
        st.caption("Raccourcis vers les hashtags les plus actifs.")
        if top_hashtags:
            render_bar_rows(
                top_hashtags,
                "hashtag",
                "tweet_count",
                placeholder,
                key_prefix="home-tags",
                on_click=lambda item: on_open_hashtag(item.get("hashtag", "")),
            )
        else:
            render_placeholder(placeholder)

    st.markdown("</div>", unsafe_allow_html=True)


def render_top10(repo, placeholder, on_open_hashtag, on_open_replies, on_open_profile, on_route_change):
    render_page_nav(on_route_change, "Top 10")
    render_page_header("Top 10", "Questions 12 et 13, plus un resume visuel pour la question 15.")
    top_mode = st.radio("Classement", ["Tweets", "Hashtags"], horizontal=True)

    if top_mode == "Tweets":
        items = get_ui_top_tweets(repo)
        render_bar_rows(
            items,
            "tweet_id",
            "favorite_count",
            placeholder,
            key_prefix="top-tweets",
            on_click=lambda item: on_open_replies(item.get("tweet_id", "")),
        )
        st.markdown("<div class='milano-divider'></div>", unsafe_allow_html=True)
        st.markdown("<div class='milano-panel-title'>Extraits</div>", unsafe_allow_html=True)
        for tweet in items[:5]:
            render_tweet_card(
                tweet,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"top-preview-{tweet.get('tweet_id', 'x')}",
            )
    else:
        items = get_ui_top_hashtags(repo)
        render_bar_rows(
            items,
            "hashtag",
            "tweet_count",
            placeholder,
            key_prefix="top-hashtags",
            on_click=lambda item: on_open_hashtag(item.get("hashtag", "")),
        )

    st.markdown("<div class='milano-divider'></div>", unsafe_allow_html=True)
    st.markdown("<div class='milano-panel-title'>Discussion la plus longue</div>", unsafe_allow_html=True)
    conversation = get_ui_longest_conversation_summary(repo)
    if not conversation:
        render_placeholder(placeholder)
        return

    info_cols = st.columns(3)
    with info_cols[0]:
        render_card("Taille", conversation.get("conversation_size", placeholder), "tweets", kind="kpi")
    with info_cols[1]:
        render_card(
            "Chaine max",
            conversation.get("longest_reply_chain_length", placeholder),
            "profondeur de reponses",
            kind="kpi",
        )
    with info_cols[2]:
        render_card("Fins", conversation.get("ending_tweet_count", placeholder), "branches terminales", kind="kpi")

    start_tweet = conversation.get("start_tweet")
    if start_tweet:
        render_tweet_card(
            start_tweet,
            placeholder,
            on_open_profile,
            on_open_replies,
            key_prefix="top-longest-conversation",
        )


def render_search(repo, placeholder, on_open_profile, on_open_hashtag, on_open_replies, on_route_change):
    render_page_nav(on_route_change, "Recherche")
    render_page_header("Recherche", "Recherche specifique par utilisateur, hashtag ou texte.")
    mode = st.radio(
        "Mode de recherche",
        ["Utilisateur", "Hashtag", "Texte"],
        horizontal=True,
        index=["Utilisateur", "Hashtag", "Texte"].index(st.session_state.search_mode),
    )
    query = st.text_input("Recherche", value=st.session_state.search_query, placeholder="Tape ta recherche")
    st.session_state.search_mode = mode
    st.session_state.search_query = query

    if not query.strip():
        st.caption("Renseigne une recherche pour afficher des resultats.")
        return

    if mode == "Utilisateur":
        rows = search_ui_users(repo, query)
        if rows == placeholder:
            render_placeholder(placeholder)
            return
        if not rows:
            st.caption("Aucun utilisateur trouve.")
            return
        for user in rows:
            cols = st.columns([3, 1])
            cols[0].markdown(
                f"**@{user.get('username', placeholder)}**  \n{user.get('role', placeholder)} • {user.get('country', placeholder)}"
            )
            if cols[1].button("Ouvrir", key=f"search-user-{user.get('user_id', '')}"):
                on_open_profile(user_id=user.get("user_id", ""), username=user.get("username", ""))
                st.rerun()

    elif mode == "Hashtag":
        rows = search_ui_hashtags(repo, query)
        render_bar_rows(
            rows,
            "hashtag",
            "tweet_count",
            placeholder,
            key_prefix="search-hashtag",
            on_click=lambda item: on_open_hashtag(item.get("hashtag", "")),
        )

    else:
        rows = search_ui_tweets_by_text(repo, query)
        if rows == placeholder:
            render_placeholder(placeholder)
            return
        if not rows:
            st.caption("Aucun tweet trouve.")
            return
        for tweet in rows:
            render_tweet_card(
                tweet,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"search-tweet-{tweet.get('tweet_id', 'x')}",
            )


def render_profile(repo, placeholder, on_open_profile, on_open_replies, on_route_change):
    render_page_nav(on_route_change, "Profil")
    render_page_header("Profil utilisateur", "Page detaillee avec tweets recents.")
    selected_user = None
    if st.session_state.selected_username:
        selected_user = get_ui_user_by_username(repo, st.session_state.selected_username)
    if not selected_user and st.session_state.selected_user_id:
        selected_user = get_ui_user_by_id(repo, st.session_state.selected_user_id)

    if not selected_user:
        lookup = st.text_input("Charger un profil", placeholder="username exact")
        if st.button("Afficher le profil", key="profile-lookup"):
            on_open_profile(username=lookup)
            st.rerun()
        st.caption("Selectionne un utilisateur depuis Recherche ou saisis un username exact.")
        return

    st.markdown(
        f"""
        <div class="milano-card">
            <div class="milano-eyebrow">Profil</div>
            <div class="milano-panel-title">@{selected_user.get('username', placeholder)}</div>
            <div class="milano-meta">{selected_user.get('role', placeholder)} • {selected_user.get('country', placeholder)}</div>
            <div class="milano-muted">Creation : {selected_user.get('created_at', placeholder)}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown("<div class='milano-panel-title'>Ses tweets</div>", unsafe_allow_html=True)
    tweets = get_ui_tweets_by_user(repo, selected_user.get("user_id", ""))
    if tweets == placeholder:
        render_placeholder(placeholder)
    elif not tweets:
        st.caption("Aucun tweet pour ce profil.")
    else:
        for tweet in tweets[:12]:
            render_tweet_card(
                tweet,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"profile-{tweet.get('tweet_id', 'x')}",
            )


def render_hashtag(repo, placeholder, on_open_profile, on_open_replies, on_route_change):
    render_page_nav(on_route_change, "Hashtag")
    render_page_header("Hashtag", "Questions 4 et 5 sur l'activite d'un hashtag.")
    render_placeholder(placeholder)


def render_replies(repo, placeholder, on_open_profile, on_open_replies, on_route_change):
    render_page_nav(on_route_change, "Reponses")
    render_page_header("Reponses", "Question 6 pour la liste globale, puis questions 14, 15 et 16 pour le detail d'une conversation.")

    st.markdown("<div class='milano-panel-title'>Q6 - Tous les tweets qui sont des reponses</div>", unsafe_allow_html=True)
    reply_tweets = get_ui_reply_tweets(repo)
    if reply_tweets == placeholder:
        render_placeholder(placeholder)
    elif not reply_tweets:
        st.caption("Aucun tweet-reponse dans le dataset.")
    else:
        st.caption("Liste globale des tweets dont `in_reply_to_tweet_id` n'est pas nul.")
        for reply_tweet in reply_tweets[:20]:
            render_tweet_card(
                reply_tweet,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"reply-list-{reply_tweet.get('tweet_id', 'x')}",
            )

    st.markdown("<div class='milano-panel-title'>Detail d'une conversation</div>", unsafe_allow_html=True)
    tweet_id = st.session_state.selected_tweet_id
    manual = st.text_input("Tweet ID", value=tweet_id, placeholder="T0001")
    if manual != tweet_id:
        st.session_state.selected_tweet_id = manual
        tweet_id = manual

    if not tweet_id.strip():
        st.caption("Ouvre un tweet depuis la liste ci-dessus, Top 10 ou Recherche.")
        return

    tweet = get_ui_tweet_by_id(repo, tweet_id)
    if not tweet:
        render_placeholder(placeholder)
        return

    st.markdown("<div class='milano-panel-title'>Tweet selectionne</div>", unsafe_allow_html=True)
    render_tweet_card(
        tweet,
        placeholder,
        on_open_profile,
        on_open_replies,
        key_prefix=f"reply-focus-{tweet.get('tweet_id', 'x')}",
        show_reply_button=False,
    )

    parent = get_ui_parent_tweet(repo, tweet)
    st.markdown("<div class='milano-panel-title'>Tweet parent</div>", unsafe_allow_html=True)
    if tweet.get("in_reply_to_tweet_id"):
        if parent:
            render_tweet_card(
                parent,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"reply-parent-{parent.get('tweet_id', 'x')}",
                show_reply_button=False,
            )
        else:
            render_placeholder(placeholder)
    else:
        st.caption("Ce tweet est deja le point de depart.")

    st.markdown("<div class='milano-panel-title'>Reponses directes</div>", unsafe_allow_html=True)
    replies = get_ui_replies_for_tweet(repo, tweet.get("tweet_id", ""))
    if replies == placeholder:
        render_placeholder(placeholder)
    elif not replies:
        st.caption("Aucune reponse directe.")
    else:
        for reply in replies:
            render_tweet_card(
                reply,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"reply-child-{reply.get('tweet_id', 'x')}",
                show_reply_button=False,
            )

    st.markdown("<div class='milano-panel-title'>Conversation etendue</div>", unsafe_allow_html=True)
    conversation = get_ui_extended_conversation(repo, tweet.get("tweet_id", ""))
    if not conversation:
        render_placeholder(placeholder)
        return

    summary_cols = st.columns(4)
    with summary_cols[0]:
        render_card(
            "Taille",
            conversation.get("conversation_size", placeholder),
            "tweets dans la conversation",
            kind="kpi",
        )
    with summary_cols[1]:
        render_card(
            "Chaine max",
            conversation.get("longest_reply_chain_length", placeholder),
            "longueur maximale de reponses",
            kind="kpi",
        )
    with summary_cols[2]:
        render_card(
            "Reponses directes",
            conversation.get("direct_reply_count", placeholder),
            "a partir du tweet initial",
            kind="kpi",
        )
    with summary_cols[3]:
        longest_label = "oui" if conversation.get("is_longest") else "non"
        render_card("Plus longue", longest_label, "sur l'ensemble du dataset", kind="kpi")

    start_tweet = conversation.get("start_tweet")
    st.markdown("<div class='milano-panel-title'>Point de depart</div>", unsafe_allow_html=True)
    if start_tweet:
        render_tweet_card(
            start_tweet,
            placeholder,
            on_open_profile,
            on_open_replies,
            key_prefix=f"reply-root-{start_tweet.get('tweet_id', 'x')}",
            show_reply_button=False,
        )
    else:
        render_placeholder(placeholder)

    st.markdown("<div class='milano-panel-title'>Fins de conversation</div>", unsafe_allow_html=True)
    ending_tweets = conversation.get("ending_tweets", [])
    if not ending_tweets:
        st.caption("Pas de fin distincte a afficher pour cette conversation.")
    else:
        for ending_tweet in ending_tweets:
            render_tweet_card(
                ending_tweet,
                placeholder,
                on_open_profile,
                on_open_replies,
                key_prefix=f"reply-ending-{ending_tweet.get('tweet_id', 'x')}",
                show_reply_button=False,
            )


def render_network(graph_context, placeholder, on_route_change):
    render_page_nav(on_route_change, "Reseau")
    render_page_header("Reseau", "Questions 7 a 11 sur les relations de suivi dans Neo4j.")

    render_question_block(
        "Q7",
        "Followers de MilanoOps",
        get_ui_q7_followers(graph_context),
        placeholder,
    )
    render_question_block(
        "Q8",
        "Utilisateurs suivis par MilanoOps",
        get_ui_q8_following(graph_context),
        placeholder,
    )
    render_question_block(
        "Q9",
        "Relations reciproques avec MilanoOps",
        get_ui_q9_mutual_connections(graph_context),
        placeholder,
    )
    render_question_block(
        "Q10",
        "Utilisateurs avec plus de 10 followers",
        get_ui_q10_users_with_more_than_ten_followers(graph_context),
        placeholder,
    )
    render_question_block(
        "Q11",
        "Utilisateurs qui suivent plus de 5 utilisateurs",
        get_ui_q11_users_following_more_than_five_users(graph_context),
        placeholder,
    )


def run_streamlit_ui():
    if get_script_run_ctx() is None:
        raise SystemExit(
            "Cette interface Streamlit doit etre lancee avec :\n"
            "python src/app_milano/main.py\n"
            "ou\n"
            "conda run -n cours streamlit run src/app_milano/utils/display.py"
        )

    st.set_page_config(page_title="Milano Streamlit", page_icon="M", layout="wide")
    load_env_file(required=False)
    init_state()
    dev_mode = os.getenv("APP_MILANO_UI_MODE") != "desktop"
    apply_styles(dev_mode=dev_mode)
    repo = create_mongo_context(PLACEHOLDER)
    graph_context = create_neo4j_context(PLACEHOLDER)

    try:
        if dev_mode:
            render_sidebar(repo, ROUTES, st.session_state.route, set_route)
        route = st.session_state.route
        if route == "Accueil":
            render_home(repo, PLACEHOLDER, open_search, open_profile, open_hashtag, open_replies, set_route)
        elif route == "Top 10":
            render_top10(repo, PLACEHOLDER, open_hashtag, open_replies, open_profile, set_route)
        elif route == "Recherche":
            render_search(repo, PLACEHOLDER, open_profile, open_hashtag, open_replies, set_route)
        elif route == "Profil":
            render_profile(repo, PLACEHOLDER, open_profile, open_replies, set_route)
        elif route == "Hashtag":
            render_hashtag(repo, PLACEHOLDER, open_profile, open_replies, set_route)
        elif route == "Reponses":
            render_replies(repo, PLACEHOLDER, open_profile, open_replies, set_route)
        else:
            render_network(graph_context, PLACEHOLDER, set_route)
    except Exception:
        st.error("Erreur de chargement de l'interface.")
        st.markdown("...........")
    finally:
        close_mongo_context(repo)
        close_neo4j_context(graph_context)


if __name__ == "__main__":
    run_streamlit_ui()
