stageleft::stageleft_crate!(flow_macro);

#[cfg(stageleft_macro)]
pub(crate) mod first_ten;
#[cfg(not(stageleft_macro))]
pub mod first_ten;

#[cfg(stageleft_macro)]
pub(crate) mod first_ten_distributed;
#[cfg(not(stageleft_macro))]
pub mod first_ten_distributed;

#[cfg(stageleft_macro)]
pub(crate) mod two_pc;
#[cfg(not(stageleft_macro))]
pub mod two_pc;

#[cfg(stageleft_macro)]
pub(crate) mod pbft;
#[cfg(not(stageleft_macro))]
pub mod pbft;