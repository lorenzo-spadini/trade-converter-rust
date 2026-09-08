use anyhow::{Result, bail};

pub const PRICE_SCALE: u32 = 12;
pub const PRICE_PRECISION: usize = 38;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub struct Price(pub i128);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExactDecimal {
    pub coefficient: i128,
    pub scale: u32,
    decimal_exponent: i32,
}

impl ExactDecimal {
    pub fn parse(text: &str) -> Result<Self> {
        let text = text.trim().replace(',', ".");
        if text.is_empty() {
            bail!("empty decimal value");
        }
        let (mantissa, exponent) = match text.find(['e', 'E']) {
            Some(index) => (
                &text[..index],
                text[index + 1..]
                    .parse::<i32>()
                    .map_err(|_| anyhow::anyhow!("invalid decimal value {text:?}"))?,
            ),
            None => (text.as_str(), 0),
        };
        let (negative, unsigned) = match mantissa.as_bytes().first() {
            Some(b'-') => (true, &mantissa[1..]),
            Some(b'+') => (false, &mantissa[1..]),
            _ => (false, mantissa),
        };
        let mut digits = String::with_capacity(unsigned.len());
        let mut fractional = 0u32;
        let mut seen_dot = false;
        for byte in unsigned.bytes() {
            match byte {
                b'0'..=b'9' => {
                    digits.push(byte as char);
                    if seen_dot {
                        fractional += 1;
                    }
                }
                b'.' if !seen_dot => seen_dot = true,
                _ => bail!("invalid decimal value {text:?}"),
            }
        }
        if digits.is_empty() {
            bail!("invalid decimal value {text:?}");
        }
        let decimal_exponent = exponent - fractional as i32;
        let coefficient = digits
            .parse::<i128>()
            .map_err(|_| anyhow::anyhow!("decimal value {text:?} exceeds i128"))?;
        let coefficient = if negative { -coefficient } else { coefficient };
        if decimal_exponent >= 0 {
            let factor = pow10(decimal_exponent as u32)?;
            Ok(Self {
                coefficient: coefficient
                    .checked_mul(factor)
                    .ok_or_else(|| anyhow::anyhow!("decimal value {text:?} exceeds i128"))?,
                scale: 0,
                decimal_exponent,
            })
        } else {
            Ok(Self {
                coefficient,
                scale: (-decimal_exponent) as u32,
                decimal_exponent,
            })
        }
    }

    pub fn is_positive(self) -> bool {
        self.coefficient > 0
    }
}

impl Price {
    pub fn parse(text: &str) -> Result<Self> {
        let value = ExactDecimal::parse(text)?;
        if value.decimal_exponent.unsigned_abs() > PRICE_SCALE {
            bail!("decimal value {text:?} has more than {PRICE_SCALE} fractional digits");
        }
        let scaled = value
            .coefficient
            .checked_mul(pow10(PRICE_SCALE - value.scale)?)
            .ok_or_else(|| anyhow::anyhow!("decimal value {text:?} exceeds decimal128"))?;
        if decimal_digits(scaled) > PRICE_PRECISION {
            bail!("decimal value {text:?} exceeds decimal128 precision");
        }
        Ok(Self(scaled))
    }

    pub fn is_aligned(self, tick: ExactDecimal) -> Result<bool> {
        if tick.scale <= PRICE_SCALE {
            let tick_scaled = tick
                .coefficient
                .checked_mul(pow10(PRICE_SCALE - tick.scale)?)
                .ok_or_else(|| anyhow::anyhow!("Tick Size exceeds supported precision"))?;
            return Ok(self.0 % tick_scaled == 0);
        }
        let price_scaled = self
            .0
            .checked_mul(pow10(tick.scale - PRICE_SCALE)?)
            .ok_or_else(|| anyhow::anyhow!("price/tick comparison exceeds i128"))?;
        Ok(price_scaled % tick.coefficient == 0)
    }
}

pub fn format_exact(value: ExactDecimal) -> String {
    if value.scale == 0 {
        return value.coefficient.to_string();
    }
    let negative = value.coefficient < 0;
    let digits = value.coefficient.unsigned_abs().to_string();
    let scale = value.scale as usize;
    let body = if digits.len() <= scale {
        format!("0.{}{}", "0".repeat(scale - digits.len()), digits)
    } else {
        let split = digits.len() - scale;
        format!("{}.{}", &digits[..split], &digits[split..])
    };
    if negative { format!("-{body}") } else { body }
}

fn decimal_digits(value: i128) -> usize {
    value.unsigned_abs().to_string().len()
}

fn pow10(power: u32) -> Result<i128> {
    10i128
        .checked_pow(power)
        .ok_or_else(|| anyhow::anyhow!("decimal scale is too large"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_exact_prices_without_float() {
        assert_eq!(Price::parse("29893,25").unwrap().0, 29_893_250_000_000_000);
        assert_eq!(Price::parse("1").unwrap().0, 1_000_000_000_000);
        assert!(Price::parse("1.0000000000001").is_err());
        assert!(Price::parse("1E+13").is_err());
    }

    #[test]
    fn tick_alignment_is_exact() {
        let tick = ExactDecimal::parse("0.25").unwrap();
        assert!(Price::parse("100.25").unwrap().is_aligned(tick).unwrap());
        assert!(!Price::parse("100.10").unwrap().is_aligned(tick).unwrap());
    }
}
