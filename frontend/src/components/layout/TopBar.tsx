import React, { useState } from 'react';
import {
  AppBar,
  Toolbar,
  IconButton,
  Typography,
  InputBase,
  Button,
  Menu,
  MenuItem,
  Box,
  Chip,
  alpha,
  styled,
} from '@mui/material';
import {
  Menu as MenuIcon,
  Search as SearchIcon,
  Add as AddIcon,
  AccountTree,
} from '@mui/icons-material';

const Search = styled('div')(({ theme }) => ({
  position: 'relative',
  borderRadius: theme.shape.borderRadius,
  backgroundColor: alpha(theme.palette.common.white, 0.15),
  '&:hover': {
    backgroundColor: alpha(theme.palette.common.white, 0.25),
  },
  marginRight: theme.spacing(2),
  marginLeft: 0,
  width: '100%',
  [theme.breakpoints.up('sm')]: {
    marginLeft: theme.spacing(3),
    width: 'auto',
  },
}));

const SearchIconWrapper = styled('div')(({ theme }) => ({
  padding: theme.spacing(0, 2),
  height: '100%',
  position: 'absolute',
  pointerEvents: 'none',
  display: 'flex',
  alignItems: 'center',
  justifyContent: 'center',
}));

const StyledInputBase = styled(InputBase)(({ theme }) => ({
  color: 'inherit',
  '& .MuiInputBase-input': {
    padding: theme.spacing(1, 1, 1, 0),
    paddingLeft: `calc(1em + ${theme.spacing(4)})`,
    transition: theme.transitions.create('width'),
    width: '100%',
    [theme.breakpoints.up('md')]: {
      width: '20ch',
      '&:focus': {
        width: '30ch',
      },
    },
  },
}));

interface TopBarProps {
  onMenuClick: () => void;
  currentBranch?: string;
}

export const TopBar: React.FC<TopBarProps> = ({ onMenuClick, currentBranch = 'main' }) => {
  const [anchorEl, setAnchorEl] = useState<null | HTMLElement>(null);
  const [branchMenuAnchor, setBranchMenuAnchor] = useState<null | HTMLElement>(null);

  const handleNewClick = (event: React.MouseEvent<HTMLElement>) => {
    setAnchorEl(event.currentTarget);
  };

  const handleClose = () => {
    setAnchorEl(null);
  };

  const handleBranchClick = (event: React.MouseEvent<HTMLElement>) => {
    setBranchMenuAnchor(event.currentTarget);
  };

  const handleBranchClose = () => {
    setBranchMenuAnchor(null);
  };

  return (
    <AppBar position="fixed" sx={{ zIndex: (theme) => theme.zIndex.drawer + 1 }}>
      <Toolbar>
        <IconButton
          color="inherit"
          aria-label="open drawer"
          edge="start"
          onClick={onMenuClick}
          sx={{ mr: 2 }}
        >
          <MenuIcon />
        </IconButton>
        
        <Typography variant="h6" noWrap component="div" sx={{ display: { xs: 'none', sm: 'block' } }}>
          온톨로지 에디터
        </Typography>

        <Search>
          <SearchIconWrapper>
            <SearchIcon />
          </SearchIconWrapper>
          <StyledInputBase
            placeholder="검색 (Cmd/Ctrl + K)"
            inputProps={{ 'aria-label': 'search' }}
          />
        </Search>

        <Box sx={{ flexGrow: 1 }} />

        <Button
          color="inherit"
          startIcon={<AddIcon />}
          onClick={handleNewClick}
          sx={{ mr: 2 }}
        >
          새로 만들기
        </Button>

        <Chip
          icon={<AccountTree />}
          label={currentBranch}
          onClick={handleBranchClick}
          variant="outlined"
          sx={{ color: 'white', borderColor: 'white' }}
        />

        <Menu
          anchorEl={anchorEl}
          open={Boolean(anchorEl)}
          onClose={handleClose}
        >
          <MenuItem onClick={handleClose}>Object Type</MenuItem>
          <MenuItem onClick={handleClose}>Link Type</MenuItem>
          <MenuItem onClick={handleClose}>Property</MenuItem>
          <MenuItem onClick={handleClose}>Group</MenuItem>
        </Menu>

        <Menu
          anchorEl={branchMenuAnchor}
          open={Boolean(branchMenuAnchor)}
          onClose={handleBranchClose}
        >
          <MenuItem onClick={handleBranchClose}>main (현재)</MenuItem>
          <MenuItem onClick={handleBranchClose}>새 브랜치 만들기...</MenuItem>
        </Menu>
      </Toolbar>
    </AppBar>
  );
};