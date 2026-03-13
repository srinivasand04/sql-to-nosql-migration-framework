class CloudCostComparator:


        def __init__(self, storage_gb, reads, writes, network_gb):
            self.storage = storage_gb
            self.reads = reads
            self.writes = writes
            self.network = network_gb

        def aws_cost(self):
            return (
                self.storage * 0.25 +
                self.reads * 0.0000003 +
                self.writes * 0.000001 +
                self.network * 0.09
            )

        def azure_cost(self):
            return (
                self.storage * 0.25 +
                self.reads * 0.00000035 +
                self.writes * 0.0000011 +
                self.network * 0.087
            )

        def gcp_cost(self):
            return (
                self.storage * 0.23 +
                self.reads * 0.00000028 +
                self.writes * 0.00000095 +
                self.network * 0.085
            )

        def digitalocean_cost(self):
            return (
                self.storage * 0.20 +
                self.network * 0.01
            )

        def print_report(self):

            aws = self.aws_cost()
            azure = self.azure_cost()
            gcp = self.gcp_cost()
            do = self.digitalocean_cost()

            costs = {
                "AWS": aws,
                "Azure": azure,
                "GCP": gcp,
                "DigitalOcean": do
            }

            print("============================================================")
            print("  CLOUD COST COMPARISON REPORT")
            print("============================================================\n")

            for provider, cost in costs.items():
                print(f"  {provider:<15} : ${cost:.4f} / month")

            best = min(costs, key=costs.get)

            print("\n------------------------------------------------------------")
            print(f"  BEST PROVIDER → {best} (${costs[best]:.4f} / month)")
            print("------------------------------------------------------------\n")

