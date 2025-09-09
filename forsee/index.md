---
title: Weekly Forsee
---

A list of weekly forsee reports.

<ul>
{% assign weeks = site.pages | where_exp: "p", "p.path contains 'forsee/'" | sort: "path" | reverse %}
{% for p in weeks %}
  {% unless p.name == "index.md" %}
  <li>
    <a href="{{ p.url }}">{{ p.path | split: "/" | last | split: "." | first }}</a>
  </li>
  {% endunless %}
{% endfor %}
</ul>
