# VPN Latency Runbook

Latency spikes after route or tunnel policy changes:
- check tunnel health and packet loss together because latency and loss often move together during path instability
- review route preference changes that may have moved traffic to a higher-latency backup path
- inspect crypto device CPU saturation if latency appears after firewall or tunnel configuration updates
- capture pre-change and post-change path metrics before recommending rollback
