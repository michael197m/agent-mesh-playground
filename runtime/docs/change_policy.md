# Change Policy

Production network rollbacks require explicit operator approval when the proposed action affects shared edge routing, firewalls, or customer-facing VPN services.

Approval checklist:
- confirm customer impact and blast radius
- identify the previous known-good configuration
- document rollback owner and validation steps
- capture approval comment before execution

Rollback guidance:
- prefer scoped rollback over blanket revert when only one route policy or ACL stanza is implicated
- validate MTU, interface error counters, and tunnel health immediately after rollback
