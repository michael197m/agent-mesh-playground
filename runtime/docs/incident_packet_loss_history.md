# Incident History

Recent packet loss incidents:
- Toronto edge packet loss after route policy change: root cause was an MTU mismatch on the WAN uplink.
- Montreal VPN packet loss after firewall update: root cause was an ACL regression blocking fragmented traffic.
- Chicago branch packet loss after emergency rollback: root cause was a partial rollback leaving asymmetric routes in place.

Lessons learned:
- compare current change set against the previous known-good revision before escalating.
- do not approve production rollback until customer impact, blast radius, and validation steps are clear.
