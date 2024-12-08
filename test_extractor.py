from pdf_extractor import extract_prices

def test_extract_prices():
    # Test case 1: Split price values
    test_row = ['Katta (Imp)', 'Rs./kg', '1', ',700.00', '1', ',700.00', '', '', '2', ',000.00 2', ',000.00', '', '', '', '', '', 'n.a.', '', 'n.a.']
    result = extract_prices(test_row)
    
    print("\nTest Case 1 - Split price values:")
    print("Input row:", test_row)
    print("Expected prices: 1700.00, 1700.00, N/A, N/A, 2000.00, 2000.00, N/A, N/A, n.a., n.a.")
    print("Actual result:", result)
    
    # Test case 2: Empty values
    test_row2 = ['Item', 'Unit', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '']
    result2 = extract_prices(test_row2)
    
    print("\nTest Case 2 - Empty values:")
    print("Input row:", test_row2)
    print("Expected all N/A")
    print("Actual result:", result2)

if __name__ == "__main__":
    test_extract_prices()
