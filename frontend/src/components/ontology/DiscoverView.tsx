import React from 'react';
import {
  Box,
  Grid,
  Card,
  CardContent,
  CardHeader,
  Typography,
  List,
  ListItem,
  ListItemButton,
  ListItemIcon,
  ListItemText,
  IconButton,
  Chip,
  Button,
  Avatar,
  Paper,
} from '@mui/material';
import {
  AccountTree,
  Star,
  StarBorder,
  History,
  MoreVert,
  Add,
  Search,
} from '@mui/icons-material';
import { OntologyClass } from '../../types/ontology';

interface DiscoverViewProps {
  favoriteClasses?: OntologyClass[];
  recentClasses?: OntologyClass[];
  allClasses?: OntologyClass[];
  onClassClick?: (classId: string) => void;
  onToggleFavorite?: (classId: string) => void;
}

export const DiscoverView: React.FC<DiscoverViewProps> = ({
  favoriteClasses = [],
  recentClasses = [],
  allClasses = [],
  onClassClick,
  onToggleFavorite,
}) => {
  const renderClassItem = (cls: OntologyClass, isFavorite: boolean = false) => (
    <ListItem
      key={cls.id}
      secondaryAction={
        <IconButton
          edge="end"
          onClick={() => onToggleFavorite?.(cls.id)}
        >
          {isFavorite ? <Star color="primary" /> : <StarBorder />}
        </IconButton>
      }
      disablePadding
    >
      <ListItemButton onClick={() => onClassClick?.(cls.id)}>
        <ListItemIcon>
          <Avatar sx={{ bgcolor: 'primary.light', width: 32, height: 32 }}>
            <AccountTree fontSize="small" />
          </Avatar>
        </ListItemIcon>
        <ListItemText
          primary={cls.label}
          secondary={
            <Box sx={{ display: 'flex', alignItems: 'center', gap: 1, mt: 0.5 }}>
              <Typography variant="caption" color="text.secondary">
                ID: {cls.id}
              </Typography>
              <Chip
                label={`${cls.properties.length} properties`}
                size="small"
                sx={{ fontSize: '0.7rem', height: 20 }}
              />
              {cls.abstract && (
                <Chip
                  label="Abstract"
                  size="small"
                  color="warning"
                  sx={{ fontSize: '0.7rem', height: 20 }}
                />
              )}
            </Box>
          }
        />
      </ListItemButton>
    </ListItem>
  );

  return (
    <Box sx={{ flexGrow: 1 }}>
      <Box sx={{ mb: 3 }}>
        <Typography variant="h4" gutterBottom>
          Discover
        </Typography>
        <Typography variant="body1" color="text.secondary">
          온톨로지 리소스를 탐색하고 관리하세요
        </Typography>
      </Box>

      {/* Quick Actions */}
      <Paper sx={{ p: 2, mb: 3 }}>
        <Box sx={{ display: 'flex', gap: 2 }}>
          <Button
            variant="contained"
            startIcon={<Add />}
            size="large"
          >
            새 Object Type
          </Button>
          <Button
            variant="outlined"
            startIcon={<Search />}
            size="large"
          >
            전체 검색
          </Button>
        </Box>
      </Paper>

      <Box sx={{ display: 'grid', gridTemplateColumns: { xs: '1fr', md: '1fr 1fr' }, gap: 3, mb: 3 }}>
        {/* Favorite Object Types */}
        <Box>
          <Card>
            <CardHeader
              title="즐겨찾는 Object Types"
              action={
                <IconButton>
                  <MoreVert />
                </IconButton>
              }
            />
            <CardContent>
              {favoriteClasses.length === 0 ? (
                <Box sx={{ textAlign: 'center', py: 3 }}>
                  <StarBorder sx={{ fontSize: 48, color: 'text.disabled' }} />
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    즐겨찾기한 Object Type이 없습니다
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    자주 사용하는 Object Type을 즐겨찾기에 추가하세요
                  </Typography>
                </Box>
              ) : (
                <List>
                  {favoriteClasses.map((cls) => renderClassItem(cls, true))}
                </List>
              )}
            </CardContent>
          </Card>
        </Box>

        {/* Recently Viewed */}
        <Box>
          <Card>
            <CardHeader
              title="최근에 본 Object Types"
              action={
                <IconButton>
                  <MoreVert />
                </IconButton>
              }
            />
            <CardContent>
              {recentClasses.length === 0 ? (
                <Box sx={{ textAlign: 'center', py: 3 }}>
                  <History sx={{ fontSize: 48, color: 'text.disabled' }} />
                  <Typography variant="body2" color="text.secondary" sx={{ mt: 1 }}>
                    최근에 본 Object Type이 없습니다
                  </Typography>
                </Box>
              ) : (
                <List>
                  {recentClasses.map((cls) => renderClassItem(cls))}
                </List>
              )}
            </CardContent>
          </Card>
        </Box>
      </Box>

      {/* All Object Types */}
      <Box>
          <Card>
            <CardHeader
              title={`모든 Object Types (${allClasses.length})`}
              action={
                <Button size="small">
                  모두 보기
                </Button>
              }
            />
            <CardContent>
              <List>
                {allClasses.slice(0, 5).map((cls) => renderClassItem(cls))}
              </List>
              {allClasses.length > 5 && (
                <Box sx={{ textAlign: 'center', mt: 2 }}>
                  <Button variant="text">
                    {allClasses.length - 5}개 더 보기
                  </Button>
                </Box>
              )}
            </CardContent>
          </Card>
      </Box>
    </Box>
  );
};