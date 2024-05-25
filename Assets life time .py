class Asset:
    def __init__(self, name, value):
        self.name = name
        self.value = value

    def calculate_depreciation(self, years):
        pass

class DepreciableAsset(Asset):
    def __init__(self, name, value, depreciation_rate):
        super().__init__(name, value)
        self.depreciation_rate = depreciation_rate

    def calculate_depreciation(self, years):
        depreciation = self.value * self.depreciation_rate * years
        return max(0,depreciation)
        
        
class NonDepreciableAsset(Asset):
        def __init__(self,name,value,depreciation_rate):
            super().__init__(name,value)
            self.depreciation_rate=depreciation_rate
        
        def calculate_depreciation(self, years):
            depreciation = self.value*self.depreciation_rate*years
            return max(0,depreciation)
        
        
# Example usage:
car = DepreciableAsset("Car", 300000, 0.01)
land = NonDepreciableAsset("Land", 2500000,0.00)

print(f"Original value of {car.name}: ${car.value}")
print(f"Depreciation in value of {car.name} after 5 years: ${car.calculate_depreciation(5)}")

print(f"Original value of {land.name}: ${land.value}")
print(f"depreciation in value of {land.name} after 5 years: ${land.calculate_depreciation(5)}")
