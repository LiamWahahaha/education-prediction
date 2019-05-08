#43.56335017, -79.62954604
from geopy.geocoders import Nominatim
from uszipcode import Zipcode
from uszipcode import SearchEngine

search = SearchEngine(simple_zipcode=False)
results = search.by_coordinates(39.122229, -77.133578, radius=10, returns=1)

print(results)
print(results[0].zipcode)
print(len(results))