"""
Shared HTTP defaults. Right now this is just user-agent strings — keep them
here rather than scattered across source-specific modules so that, when one
upstream starts rejecting a UA we use, we have a single place to add a new
named variant and re-route callers to it.
"""

USER_AGENTS = {
    # Plausible desktop-Chrome UA. Several mortality/demography sites (HMD
    # included) 403 obviously-bot user-agents, so default to this for any
    # source that doesn't publish a friendlier scraping policy.
    "default": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
}


def user_agent(name: str = "default") -> str:
    try:
        return USER_AGENTS[name]
    except KeyError:
        raise ValueError(
            f"Unknown user-agent {name!r}. Known: {sorted(USER_AGENTS)}"
        ) from None
