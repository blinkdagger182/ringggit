//
//  PIIRedactionTests.swift
//  dimeTests
//
//  Tests for PIIRedactionValidators — pure logic, no Vision/UIKit required.
//  To run: File > New > Target > Unit Testing Bundle (name it "dimeTests"), then
//  add this file to that target. Mark @testable import dime at the top.
//

import XCTest
@testable import dime

final class PIIRedactionTests: XCTestCase {

    // MARK: - IC Date Validation

    func test_ICDate_validDates() {
        // Common Malaysian birth dates
        XCTAssertTrue(PIIRedactionValidators.isValidICDate("800101"))   // Jan 1, 1980
        XCTAssertTrue(PIIRedactionValidators.isValidICDate("901231"))   // Dec 31, 1990
        XCTAssertTrue(PIIRedactionValidators.isValidICDate("010615"))   // Jun 15, 2001
        XCTAssertTrue(PIIRedactionValidators.isValidICDate("990229"))   // Feb 29 (we don't require calendar accuracy)
        XCTAssertTrue(PIIRedactionValidators.isValidICDate("031112"))   // Nov 12, 2003
    }

    func test_ICDate_invalidMonth() {
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("801301"))  // month 13
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("800001"))  // month 0
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("991500"))  // month 15
    }

    func test_ICDate_invalidDay() {
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("800100"))  // day 0
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("800132"))  // day 32
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("801299"))  // day 99
    }

    func test_ICDate_wrongLength() {
        XCTAssertFalse(PIIRedactionValidators.isValidICDate(""))
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("80010"))     // 5 chars
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("8001011"))   // 7 chars
    }

    func test_ICDate_nonNumeric() {
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("YYMMDD"))
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("ab0101"))
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("80-101"))
    }

    // MARK: - Financial amounts should NOT have a valid IC date prefix

    func test_ICDate_financialAmounts_notValidIC() {
        // Amounts like 12345.67 → digits "1234567", not 12 digits → regex won't match anyway
        // But test that common 6-digit financial prefixes fail the date check
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("000000"))  // zeros — day 0 invalid
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("991300"))  // month 13
        XCTAssertFalse(PIIRedactionValidators.isValidICDate("991332"))  // month 13, day 32
    }

    // MARK: - Luhn Algorithm

    func test_luhn_validCards() {
        // Standard test vectors
        let visa       = [4, 5, 3, 2, 0, 1, 5, 1, 1, 2, 8, 3, 0, 3, 6, 6]   // 4532015112830366
        let mastercard = [5, 4, 2, 5, 2, 3, 3, 4, 3, 0, 1, 0, 9, 9, 0, 3]   // 5425233430109903
        let amex       = [3, 7, 8, 2, 8, 2, 2, 4, 6, 3, 1, 0, 0, 0, 5]       // 378282246310005

        XCTAssertTrue(PIIRedactionValidators.luhnCheck(visa))
        XCTAssertTrue(PIIRedactionValidators.luhnCheck(mastercard))
        XCTAssertTrue(PIIRedactionValidators.luhnCheck(amex))
    }

    func test_luhn_invalidCards() {
        let invalid1 = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2, 3, 4, 5, 6]   // 1234567890123456
        let invalid2 = [4, 5, 3, 2, 0, 1, 5, 1, 1, 2, 8, 3, 0, 3, 6, 7]   // one digit off

        XCTAssertFalse(PIIRedactionValidators.luhnCheck(invalid1))
        XCTAssertFalse(PIIRedactionValidators.luhnCheck(invalid2))
    }

    func test_luhn_tooShort() {
        // < 13 digits → always false
        XCTAssertFalse(PIIRedactionValidators.luhnCheck([4, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]))  // 12 digits
        XCTAssertFalse(PIIRedactionValidators.luhnCheck([]))
    }

    func test_luhn_bankStatementAmounts_fail() {
        // A 12-digit bank account-like number that is NOT a Luhn-valid card
        let nonCard = [1, 2, 3, 4, 5, 6, 7, 8, 9, 0, 1, 2]
        XCTAssertFalse(PIIRedactionValidators.luhnCheck(nonCard))
    }

    // MARK: - Digits Only

    func test_digitsOnly_stripsNonDigits() {
        XCTAssertEqual(PIIRedactionValidators.digitsOnly("800101-14-5432"), "800101145432")
        XCTAssertEqual(PIIRedactionValidators.digitsOnly("800101 14 5432"), "800101145432")
        XCTAssertEqual(PIIRedactionValidators.digitsOnly("RM 1,234.56"),    "123456")
        XCTAssertEqual(PIIRedactionValidators.digitsOnly(""),               "")
        XCTAssertEqual(PIIRedactionValidators.digitsOnly("abc"),            "")
    }

    // MARK: - IC Regex Pattern

    func test_ICPattern_matchesValidFormats() {
        // Pattern: (\d{6})[- ]?(\d{2})[- ]?(\d{4})
        let pattern = #"(?<!\d)(\d{6})[- ]?(\d{2})[- ]?(\d{4})(?!\d)"#
        let regex = try! NSRegularExpression(pattern: pattern)

        let validInputs = [
            "800101-14-5432",    // standard with hyphens
            "800101145432",      // no separators
            "901231 12 3456",    // with spaces
        ]

        for input in validInputs {
            let matches = regex.matches(in: input, range: NSRange(input.startIndex..., in: input))
            XCTAssertFalse(matches.isEmpty, "Should match: \(input)")

            // Verify YYMMDD portion is a valid date
            let digits = PIIRedactionValidators.digitsOnly(input)
            XCTAssertEqual(digits.count, 12, "IC should be 12 digits: \(input)")
            XCTAssertTrue(PIIRedactionValidators.isValidICDate(String(digits.prefix(6))),
                         "YYMMDD should be valid for: \(input)")
        }
    }

    func test_ICPattern_rejectsInvalidDates() {
        // Even if regex matches, YYMMDD validation should reject these
        let inputs = [
            ("801301145432", "month 13"),   // YYMMDD = 801301
            ("800001145432", "month 0"),    // YYMMDD = 800001
            ("800100145432", "day 0"),      // YYMMDD = 800100
        ]

        for (digits, desc) in inputs {
            let isValid = PIIRedactionValidators.isValidICDate(String(digits.prefix(6)))
            XCTAssertFalse(isValid, "Should reject \(desc): \(digits)")
        }
    }

    // MARK: - Bank Transaction References NOT IC

    func test_bankReferences_notIC() {
        // Typical bank ref codes — alphanumeric, so the 12-digit IC regex won't match at all
        let refs = [
            "FT23110112345678",
            "IBG2311011234",
            "TT230101ABC1234",
            "DUITNOW20231101",
        ]

        let pattern = #"(?<!\d)(\d{6})[- ]?(\d{2})[- ]?(\d{4})(?!\d)"#
        let regex = try! NSRegularExpression(pattern: pattern)

        for ref in refs {
            let range = NSRange(ref.startIndex..., in: ref)
            let matches = regex.matches(in: ref, range: range)
            // Should not extract a 12-digit pure-numeric match
            let pureNumericMatch = matches.contains { m in
                guard let r = Range(m.range, in: ref) else { return false }
                return PIIRedactionValidators.digitsOnly(String(ref[r])).count == 12
            }
            XCTAssertFalse(pureNumericMatch, "Ref '\(ref)' should not match IC pattern as pure 12 digits")
        }
    }

    // MARK: - Phone Number Regex

    func test_phoneRegex_matchesMalaysianMobiles() {
        let pattern = #"(?<!\d)(\+?6?01[0-9][-\s]?\d{7,8})(?!\d)"#
        let regex = try! NSRegularExpression(pattern: pattern)

        let validPhones = [
            "+60123456789",
            "60123456789",
            "0123456789",
            "012-3456789",
            "011-12345678",
        ]

        for phone in validPhones {
            let range = NSRange(phone.startIndex..., in: phone)
            let matches = regex.matches(in: phone, range: range)
            XCTAssertFalse(matches.isEmpty, "Should match phone: \(phone)")
        }
    }

    func test_phoneRegex_rejectsNonPhones() {
        let pattern = #"(?<!\d)(\+?6?01[0-9][-\s]?\d{7,8})(?!\d)"#
        let regex = try! NSRegularExpression(pattern: pattern)

        let nonPhones = [
            "1234567890",       // US number — no 01x prefix
            "RM 600.00",        // Amount starting with 6
            "01/01/2023",       // Date
            "Account 0123",     // Too short after 01
        ]

        for input in nonPhones {
            let range = NSRange(input.startIndex..., in: input)
            let matches = regex.matches(in: input, range: range)
            XCTAssertTrue(matches.isEmpty, "Should NOT match: \(input)")
        }
    }

    // MARK: - Email Regex

    func test_emailRegex_matchesValidEmails() {
        let pattern = #"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"#
        let regex = try! NSRegularExpression(pattern: pattern)

        let validEmails = [
            "user@example.com",
            "john.doe+tag@company.co.uk",
            "test123@mail.my",
        ]

        for email in validEmails {
            let range = NSRange(email.startIndex..., in: email)
            XCTAssertFalse(regex.matches(in: email, range: range).isEmpty, "Should match: \(email)")
        }
    }

    func test_emailRegex_rejectsNonEmails() {
        let pattern = #"[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}"#
        let regex = try! NSRegularExpression(pattern: pattern)

        let nonEmails = [
            "not-an-email",
            "missing@tld",
            "@nodomain.com",
            "RM 1,234.56",
        ]

        for input in nonEmails {
            let range = NSRange(input.startIndex..., in: input)
            XCTAssertTrue(regex.matches(in: input, range: range).isEmpty, "Should NOT match: \(input)")
        }
    }

    // MARK: - Amounts Should Not Look Like IC

    func test_amounts_notIC() {
        // Bank statement amounts are never 12 consecutive digits
        let amounts = [
            "1,234.56",
            "RM 500.00",
            "12.50",
            "100,000.00",
        ]

        for amount in amounts {
            let digits = PIIRedactionValidators.digitsOnly(amount)
            // IC requires exactly 12 digits — amounts never produce 12 digits
            XCTAssertNotEqual(digits.count, 12, "Amount '\(amount)' should not be 12 digits")
        }
    }

    // MARK: - Date Strings Should Not Be IC

    func test_dates_notIC() {
        // Dates in common bank statement formats
        let dates = [
            "01/01/2024",   // 8 digits → not 12
            "2024-01-01",   // 8 digits
            "01 Jan 2024",  // non-numeric month
            "31 Dec 2023",
        ]

        let pattern = #"(?<!\d)(\d{6})[- ]?(\d{2})[- ]?(\d{4})(?!\d)"#
        let regex = try! NSRegularExpression(pattern: pattern)

        for date in dates {
            let range = NSRange(date.startIndex..., in: date)
            let matches = regex.matches(in: date, range: range)
            let hasValidICMatch = matches.contains { m in
                guard let r = Range(m.range, in: date) else { return false }
                let digits = PIIRedactionValidators.digitsOnly(String(date[r]))
                return digits.count == 12 && PIIRedactionValidators.isValidICDate(String(digits.prefix(6)))
            }
            XCTAssertFalse(hasValidICMatch, "Date '\(date)' should not be detected as IC")
        }
    }

    // MARK: - Random 12-digit Reference Not IC

    func test_random12Digits_failsICDateValidation() {
        // A random 12-digit string where the first 6 don't form a valid YYMMDD
        let randomRefs = [
            "991599123456",   // YYMMDD=991599: month 15, day 99 → invalid
            "000000123456",   // YYMMDD=000000: month 0, day 0 → invalid
            "999999999999",   // YYMMDD=999999: month 99, day 99 → invalid
        ]

        for ref in randomRefs {
            let datePrefix = String(ref.prefix(6))
            XCTAssertFalse(PIIRedactionValidators.isValidICDate(datePrefix),
                          "Random ref '\(ref)' should fail IC date validation")
        }
    }

    // MARK: - autoEnableThreshold

    func test_autoEnableThreshold_highConfidenceItemEnabled() {
        let item = RedactionItem(
            redactionType: .email,
            pageIndex: 0,
            boundingBox: .zero,
            confidenceScore: 0.97,
            detectionReason: "Test"
        )
        XCTAssertTrue(item.isEnabled)  // 0.97 >= 0.95 threshold
    }

    func test_autoEnableThreshold_lowConfidenceItemDisabled() {
        let item = RedactionItem(
            redactionType: .email,
            pageIndex: 0,
            boundingBox: .zero,
            confidenceScore: 0.50,
            detectionReason: "Test"
        )
        XCTAssertFalse(item.isEnabled)  // 0.50 < 0.95 threshold
    }

    func test_autoEnableThreshold_icAboveThreshold() {
        // IC threshold is 0.95 — only enabled when both YYMMDD valid AND IC context present (0.60 + 0.37 = 0.97)
        let highConfIC = RedactionItem(
            redactionType: .icPassport,
            pageIndex: 0,
            boundingBox: .zero,
            confidenceScore: 0.97,
            detectionReason: "IC + context"
        )
        XCTAssertTrue(highConfIC.isEnabled)

        // Without context boost: 0.60 — below 0.95 → suggested review
        let lowConfIC = RedactionItem(
            redactionType: .icPassport,
            pageIndex: 0,
            boundingBox: .zero,
            confidenceScore: 0.60,
            detectionReason: "IC no context"
        )
        XCTAssertFalse(lowConfIC.isEnabled)
    }
}
