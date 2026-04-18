# Network Change Runbook

Packet loss after a config update:
- verify interface error counters on the affected edge device
- compare the new config against the previous known-good version
- rollback the change if impact is ongoing or packet loss exceeds the change window threshold
- inspect MTU, routing, ACLs, and VPN tunnel health before approving further remediation

Route policy regressions:
- inspect prefix match order and recent route-map changes
- confirm tunnel failover policy did not shift traffic to a degraded path
- validate asymmetric routing is not causing selective packet drops
