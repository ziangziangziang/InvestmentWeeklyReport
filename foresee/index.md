---
title: Weekly Foresee
---

A list of weekly foresee reports.

<ul>
{% assign weeks = site.pages | where_exp: "p", "p.path contains 'foresee/'" | sort: "path" | reverse %}
{% for p in weeks %}
  {% unless p.name == "index.md" %}
  <li>
    <a href="{{ p.url | relative_url }}">{{ p.path | split: "/" | last | split: "." | first }}</a>
  </li>
  {% endunless %}
{% endfor %}
</ul>
